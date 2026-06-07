from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Sequence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass
class CleanOptions:
    """
    Configura les passes de neteja --> `Cleaner.clean()`.
    Si no es passa res, s'utilitzen els valors per defecte.
    """

    # Elimina espais al principi/final de les columnes de text (StringType).
    trim_strings: bool = True
    # Després del trim (si aplica), converteix cadena buida "" en NULL de Spark.
    empty_string_as_null: bool = True
    # Elimina files duplicades (mateix contingut a les columnes considerades).
    drop_duplicates: bool = True
    # Si és None, Spark compara totes les columnes per decidir duplicat.
    # Si poses p. ex. ("id", "dia"), dues files amb mateix id+dia es consideren duplicat.
    duplicate_subset: Optional[Sequence[str]] = None
    # Si True, esborra files on TOTES les columnes són NULL (files "buides").
    drop_rows_all_null: bool = False



class Cleaner:
    """
    Orquestra Lectura Delta → transformacions de neteja → Escriptura Delta.
    Rep la SparkSession configurada (master, S3A, extensió Delta, etc.).
    """

    def __init__(self, spark: SparkSession) -> None:
        # Guardem la sessió per a read/write i per exposar-la si cal fora.
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Accés de només lectura a la mateixa SparkSession injectada al constructor."""
        return self._spark

    def read_delta(self, path: str) -> DataFrame:
        """Llegeix el DataFrame amb Delta."""
        return self._spark.read.format("delta").load(path)
    
    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[Sequence[str]] = None) -> None:
        """Escriu el DataFrame com a taula Delta a path. Sanititza noms de columnes abans d'escriure."""
        df = self._sanitize_column_names(df)
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    def clean(self, df: DataFrame, options: Optional[CleanOptions] = None) -> DataFrame:
        """
        Aplica les regles de CleanOptions en ordre: strings → files buides → deduplicar.
        Retorna un nou pla lògic (DataFrame); fins que no es fa count/write, Spark no executa.
        Bàsicament aplica les opcions de neteja passades al dataframe.
        """
        opts = options or CleanOptions()
        out = df

        # 1) Normalitza columnes de text segons els flags (trim i/o "" → NULL).
        if opts.trim_strings or opts.empty_string_as_null:
            out = self._normalize_string_columns(out, trim=opts.trim_strings, empty_as_null=opts.empty_string_as_null)

        # 2) Opcional: elimina files on no queda cap dada (totes les cel·les NULL).
        if opts.drop_rows_all_null:
            # Construïm una condició OR: almenys una columna ha de ser no nula.
            cond = None
            for name in out.columns:
                c = F.col(name).isNotNull()
                cond = c if cond is None else (cond | c)
            if cond is not None:
                out = out.filter(cond)

        # 3) Opcional: deduplicar (per totes les columnes o només per `duplicate_subset`).
        if opts.drop_duplicates:
            subset = list(opts.duplicate_subset) if opts.duplicate_subset else None
            out = out.dropDuplicates(subset=subset)

        return out

    @staticmethod
    def _sanitize_column_names(df: DataFrame) -> DataFrame:
        """Substitueix caràcters invàlids per Delta ( ,;{}()\n\t=) per guió baix."""
        import re
        invalid = re.compile(r'[ ,;{}\(\)\n\t=]+')
        out = df
        for field in out.schema.fields:
            clean = invalid.sub("_", field.name).strip("_")
            if clean != field.name:
                out = out.withColumnRenamed(field.name, clean)
        return out

    def clean_delta(self, input_path: str, output_path: str, options: Optional[CleanOptions] = None, write_mode: str = "overwrite",
        partition_by: Optional[Sequence[str]] = None) -> DataFrame:
        """
        Llegeix Delta des de input_path, neteja amb clean() i escriu Delta a output_path.
        """
        df = self.read_delta(input_path)
        cleaned = self.clean(df, options=options)
        self.write_delta(cleaned, output_path, mode=write_mode, partition_by=partition_by)
        return cleaned

    @staticmethod
    def _normalize_string_columns(df: DataFrame, *, trim: bool, empty_as_null: bool) -> DataFrame:
        """
        Només toca columnes el tipus de les quals és StringType en l'esquema.
        Recorre l'esquema per no intentar trim/nullif en números o structs; evita errors de types.
        """
        out = df
        for field in out.schema.fields:
            if not isinstance(field.dataType, StringType):
                continue
            col = F.col(field.name)
            # Opcionalment retallem espais; si trim=False, usem la columna tal qual.
            expr = F.trim(col) if trim else col
            # when(expr == "", None) retorna NULL quan el resultat és cadena buida.
            if empty_as_null:
                expr = F.when(expr == "", None).otherwise(expr)
            out = out.withColumn(field.name, expr)
        return out
