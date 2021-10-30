from itertools import chain
from sys import stdout
from typing import List

from pyspark.sql import SparkSession

from spark_ddl_handler.loader.load import Cluster


def to_stdout(spark: SparkSession, schema_names: List[str]):
    cluster = Cluster(schema_names)

    cluster.load_schemas(spark)

    tables = (schema.tables for schema in cluster.schemas)

    ddl = '\n'.join(
        table.ddl
        for table
        in chain(*tables)
    )

    stdout.write(ddl)
