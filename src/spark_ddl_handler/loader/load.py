from dataclasses import dataclass, field
from typing import List, Iterable

from pyspark.sql import SparkSession


@dataclass
class Cluster:
    schema_names: Iterable[str]
    schemas: List['Schema'] = field(default_factory=list)

    def load_schemas(self, spark: SparkSession):
        schemas = []
        for schema_name in self.schema_names:
            schema = Schema(schema_name)
            schema.load_tables(spark)
            schemas.append(schema)
        self.schemas = schemas
        return self


@dataclass
class Schema:
    schema: str
    tables: List['Table'] = field(default_factory=list)

    def show_tables(self, spark: SparkSession):
        return spark.sql(f'show tables in {self.schema}').collect()

    def load_tables(self, spark: SparkSession):
        tables = []
        for table_row in self.show_tables(spark):
            table = Table(self.schema, table_row.tableName)
            table.load_ddl(spark)
            tables.append(table)
        self.tables = tables
        return self


@dataclass
class Table:
    schema: str
    table: str
    ddl: str = field(default='')

    @property
    def name(self):
        return f'{self.schema}.{self.table}'

    def load_ddl(self, spark: SparkSession):
        row = spark.sql(f'show create table {self.schema}.{self.table}').take(1)
        self.ddl = row[0][0]
        return self
