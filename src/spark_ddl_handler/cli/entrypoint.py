from argparse import Namespace

from pyspark import SparkConf
from pyspark.sql import SparkSession

from spark_ddl_handler.loader.write import to_stdout
from spark_ddl_handler.parser.write import to_python_package


class CLIEntrypoint:
    def __init__(self, command: str):
        self.command = command

    def __call__(self, args: Namespace):
        if self.command == 'load':
            with self.spark_session(args) as spark:
                to_stdout(spark, args.schema_names)
        elif self.command == 'parse':
            to_python_package(args.input, args.output)
        else:
            raise ValueError(f'The "{self.command}" is not supported yet.')

    @classmethod
    def spark_session(cls, args: Namespace):
        spark_conf = SparkConf().setAll(args.spark_conf)
        return SparkSession.builder.config(conf=spark_conf).getOrCreate()
