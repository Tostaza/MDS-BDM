from __future__ import annotations

"""
Utilidades para la capa "trusted": leer Parquet, aplicar reglas de calidad de datos
con Spark y volver a escribir Parquet. Pensado para rutas locales o S3A (MinIO, SeaweedFS, etc.).
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
    # Si no es None, `dropna` de Spark: borra filas con NULL en esas columnas.
    dropna_subset: Optional[Sequence[str]] = None
    # Cómo cuenta los NULL en dropna: "any" = basta un NULL en el subset; "all" = todos NULL en el subset.
    dropna_how: str = "any"


class Cleaner:
    """
    Orquesta lectura Parquet → transformaciones de limpieza → escritura Parquet.
    Recibe la SparkSession ya configurada (master, S3A, etc.); esta clase no crea el cluster.
    """

    def __init__(self, spark: SparkSession) -> None:
        # Guardamos la sesión para read/write y para exponerla si hace falta fuera.
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Acceso de solo lectura a la misma SparkSession inyectada en el constructor."""
        return self._spark

    def read_parquet(self, path: str) -> DataFrame:
        """
        Carga un directorio o archivo Parquet en un DataFrame lazy (no ejecuta hasta una acción).
        `path` puede ser ruta local (`file:///...`) o objeto store (`s3a://bucket/prefijo/`).
        """
        return self._spark.read.parquet(path)

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

        # 3) Opcional: dropna sobre columnas críticas (claves, fechas obligatorias, etc.).
        if opts.dropna_subset:
            out = out.dropna(how=opts.dropna_how, subset=list(opts.dropna_subset))

        # 4) Opcional: deduplicar (por todas las columnas o solo por `duplicate_subset`).
        if opts.drop_duplicates:
            subset = list(opts.duplicate_subset) if opts.duplicate_subset else None
            out = out.dropDuplicates(subset=subset)

        return out

    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Materializa el DataFrame en formato Parquet en `path`.
        - `mode`: típico "overwrite" (reemplaza datos en esa ruta) o "append".
        - `partition_by`: si indicas columnas, Spark crea subcarpetas por valor (particionado Hive-style).
        """
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)

    def clean_parquet(
        self,
        input_path: str,
        output_path: str,
        options: Optional[CleanOptions] = None,
        write_mode: str = "overwrite",
        partition_by: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """
        Atajo: lee desde `input_path`, limpia con `clean()` y escribe en `output_path`.
        El `return` es el DataFrame limpio por si quisieramos hacer en algun punto del codigo `.count()`, tests o un paso siguiente.
        La escritura en disco ya se dispara aquí (acción de write).
        """
        df = self.read_parquet(input_path)
        cleaned = self.clean(df, options=options)
        self.write_parquet(cleaned, output_path, mode=write_mode, partition_by=partition_by)
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
