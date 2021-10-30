from pathlib import PosixPath
from sys import stdin
from shutil import rmtree
from logging import getLogger

from spark_ddl_handler.parser.parse import HiveDDLParser

LOG = getLogger(__name__)


def to_python_package(inp: str, output: PosixPath):
    if inp == ['-']:
        if stdin.isatty():
            raise ValueError('Supply some input when reading from STDIN.')

        file = stdin.read()
    else:
        with open(inp, 'r', encoding='utf8') as fd:
            file = fd.read()

    parser = HiveDDLParser()
    for ddl in file.split('\n\n'):
        parser.parse(ddl)

    tables = parser.tables

    rmtree(output.absolute().as_posix(), ignore_errors=True)

    for table in tables:
        (output / table.db_name.value).mkdir(parents=True, exist_ok=True)
        table_module = output / table.db_name.value / f'{table.table_name.value}.py'
        table_module.write_text(str(table))
        LOG.info('Has written "%s".', table_module.as_posix())
