from __future__ import annotations

"""
Utilidades para la capa "trusted": leer **tablas Delta**, aplicar reglas de calidad de datos
con Spark y volver a escribir **Delta**. Pensado para rutas locales o S3A (MinIO, SeaweedFS, etc.).

Nota d'arquitectura: el bronze persistent (``persistent-landing-zone``) ja són taules Delta
escrites des d'Airflow amb ``delta-rs`` (paquet ``deltalake``). Aquí llegim com a Delta perquè
el log transaccional (``_delta_log``) ens diu quins fitxers Parquet són vigents — si en lloc
d'això llegíssim com a Parquet pla, podríem absorbir fitxers orfes d'overwrites anteriors i
acabar amb files duplicades. L'escriptura també és Delta perquè així la capa Gold
(exploitation) hereta el mateix avantatge: ACID, ``overwrite`` segur, time-travel.
"""

from dataclasses import dataclass
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass
class CleanOptions:
    """
    Configura qué pasos de limpieza se ejecutan en `Cleaner.clean()`.
    Si no pasas nada, se usan los valores por defecto del dataclass.
    """

    # Quitar espacios al inicio/fin en columnas de texto (StringType).
    trim_strings: bool = True
    # Tras el trim (si aplica), convertir cadena vacía "" en NULL de Spark.
    empty_string_as_null: bool = True
    # Eliminar filas duplicadas (mismo contenido en las columnas consideradas).
    drop_duplicates: bool = True
    # Si es None, Spark compara todas las columnas para decidir duplicado.
    # Si pones p. ej. ("id", "dia"), dos filas con mismo id+dia se consideran duplicado.
    duplicate_subset: Optional[Sequence[str]] = None
    # Si True, borra filas donde TODAS las columnas son NULL (filas “vacías”).
    drop_rows_all_null: bool = False
    # Cómo cuenta los NULL en dropna: "any" = basta un NULL en el subset; "all" = todos NULL en el subset.
    dropna_how: str = "any"


class Cleaner:
    """
    Orquesta lectura Delta → transformaciones de limpieza → escritura Delta.
    Recibe la SparkSession ya configurada (master, S3A, extensión Delta, etc.); esta clase no crea el cluster.
    """

    def __init__(self, spark: SparkSession) -> None:
        # Guardamos la sesión para read/write y para exponerla si hace falta fuera.
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Acceso de solo lectura a la misma SparkSession inyectada en el constructor."""
        return self._spark

    def read_delta(self, path: str) -> DataFrame:
        """
        Carga una tabla Delta en un DataFrame lazy (no ejecuta hasta una acción).

        `path` puede ser ruta local (`file:///...`) o objeto store (`s3a://bucket/prefijo/`).
        El reader Delta consulta ``_delta_log`` para devolver SOLO los ficheros parquet vigentes
        (no los orfes de overwrites previos).
        """
        return self._spark.read.format("delta").load(path)

    def clean(self, df: DataFrame, options: Optional[CleanOptions] = None) -> DataFrame:
        """
        Aplica las reglas de `CleanOptions` en orden: strings → filas vacías → dropna → deduplicar.
        Devuelve un nuevo plan lógico (DataFrame); hasta que no hagas count/write, Spark no ejecuta.
        Bàsciamente lo que hace es aplicar las opciones de limpieza que se le pasan, aplicadas al dataframe.
        """
        opts = options or CleanOptions()
        out = df

        # 1) Normalizar columnas de texto según flags (trim y/o "" → NULL).
        if opts.trim_strings or opts.empty_string_as_null:
            out = self._normalize_string_columns(out, trim=opts.trim_strings, empty_as_null=opts.empty_string_as_null)

        # 2) Opcional: quitar filas donde no queda ningún dato (todas las celdas NULL).
        if opts.drop_rows_all_null:
            # Construimos una condición OR: al menos una columna debe ser no nula.
            cond = None
            for name in out.columns:
                c = F.col(name).isNotNull()
                cond = c if cond is None else (cond | c)
            if cond is not None:
                out = out.filter(cond)

        # 4) Opcional: deduplicar (por todas las columnas o solo por `duplicate_subset`).
        if opts.drop_duplicates:
            subset = list(opts.duplicate_subset) if opts.duplicate_subset else None
            out = out.dropDuplicates(subset=subset)

        return out

    def write_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Materializa el DataFrame como **tabla Delta** en `path`.

        - `mode`: típico "overwrite" (reemplaza la versión activa en ``_delta_log``) o "append".
          Con Delta, "overwrite" es atómico: si el job casca a mitja escriptura el log no es promociona
          i els lectors continuen veient la versió anterior. Amb Parquet pla, en canvi, podies acabar
          amb mitja carpeta sobreescrita i mitja antiga.
        - `partition_by`: subcarpetes Hive-style por valor (igual que en Parquet).
        - ``overwriteSchema=true``: si l'esquema canvia (p. ex. la pipeline afegeix una columna)
          Delta refusa l'overwrite per defecte; aquí volem que la trusted-zone es regeneri sense fricció.
        """
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    def clean_delta(
        self,
        input_path: str,
        output_path: str,
        options: Optional[CleanOptions] = None,
        write_mode: str = "overwrite",
        partition_by: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """
        Atajo: lee Delta desde `input_path`, limpia con `clean()` y escribe Delta en `output_path`.
        El `return` es el DataFrame limpio por si quisieramos hacer en algun punto del codigo `.count()`, tests o un paso siguiente.
        La escritura en disco ya se dispara aquí (acción de write).
        """
        df = self.read_delta(input_path)
        cleaned = self.clean(df, options=options)
        self.write_delta(cleaned, output_path, mode=write_mode, partition_by=partition_by)
        return cleaned

    @staticmethod
    def _normalize_string_columns(df: DataFrame, *, trim: bool, empty_as_null: bool) -> DataFrame:
        """
        Solo toca columnas cuyo tipo es StringType en el schema.
        Recorre el schema para no intentar trim/nullif en números o structs; evita errores de tipos.
        Bàsicamente lo que hace es mira si en el catalogo de options hay que trimar o nullifar las columnas de texto.
        En el caso que si haya que aplicar ese tratamiento, lo hace para todas las columnas de texto del dataframe.
        """
        out = df
        for field in out.schema.fields:
            if not isinstance(field.dataType, StringType):
                continue
            col = F.col(field.name)
            # Primero opcionalmente recortamos espacios; si trim=False, usamos la columna tal cual.
            expr = F.trim(col) if trim else col
            # nullif(expr, "") devuelve NULL cuando el resultado es cadena vacía.
            if empty_as_null:
                expr = F.when(expr == "", None).otherwise(expr)
            out = out.withColumn(field.name, expr)
        return out
