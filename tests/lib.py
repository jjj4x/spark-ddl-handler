from re import compile as re_compile, IGNORECASE, MULTILINE
from typing import Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

PATTERN_TABLE = re_compile(r'.* {schema}.(\w+)\s*\(.*', IGNORECASE | MULTILINE)

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
DDL_TEMPLATE_COUNTRY_CODE = """
create table {schema}.country_code (
    id int,
    country_name varchar(255),
    country_code varchar(2)
)
"""

# noinspection SqlNoDataSourceInspection
DDL_TEMPLATE_PERMISSIONS = """
create table {schema}.permissions (
    id int,
    group_name string,
    permission_name string
)

stored as parquet
"""


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class BaseTestCase:
    spark: SparkSession
    schema: Optional[str] = None

    @classmethod
    def recreate_test_schema(cls, schema: str):
        cls.spark.sql(f'create schema if not exists {schema}')

    @classmethod
    def recreate_test_table(cls, schema: str, ddl_template: str):
        table = PATTERN_TABLE.match(ddl_template.strip()).group(1)
        cls.spark.sql(f'drop table if exists {schema}.{table}')
        cls.spark.sql(ddl_template.format(schema=schema))

    @classmethod
    def setup_class(cls):
        spark_conf = SparkConf().set('spark.app.name', cls.__name__)
        cls.spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

        schema = cls.schema
        if schema is not None:
            cls.recreate_test_schema(schema)
            cls.recreate_test_table(schema, DDL_TEMPLATE_COUNTRY_CODE)
            cls.recreate_test_table(schema, DDL_TEMPLATE_PERMISSIONS)

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()
