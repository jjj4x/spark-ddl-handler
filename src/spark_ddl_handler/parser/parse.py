from logging import getLogger
from pathlib import PosixPath
from typing import Tuple, Optional, List, Union, Set

from lark import Transformer, Token, Visitor, Lark

from spark_ddl_handler.representation.python import (
    FileFormat,
    PrimitiveType,
    Column,
    Columns,
    TableName,
    DBName,
    Table,
    FullColumnName,
)

LOG = getLogger(__name__)


class HiveDDLTransformer(Transformer):
    INT = int
    NUMBER = float
    STRING = str

    @staticmethod
    def cname(name: Token):
        return name.value

    CNAME = cname

    @staticmethod
    def col_name(el):
        return el[0]

    @staticmethod
    def class_name(el):
        return '.'.join(el)

    @staticmethod
    def file_format_sequencefile(_):
        return 'SEQUENCEFILE'

    @staticmethod
    def file_format_textfile(_):
        return 'TEXTFILE'

    @staticmethod
    def file_format_rcfile(_):
        return 'RCFILE'

    @staticmethod
    def file_format_orc(_):
        return 'ORC'

    @staticmethod
    def file_format_parquet(_):
        return 'PARQUET'

    @staticmethod
    def file_format_avro(_):
        return 'AVRO'

    @staticmethod
    def file_format_jsonfile(_):
        return 'JSONFILE'

    @staticmethod
    def file_format_custom(el):
        return 'custom', el[0], el[1]

    @staticmethod
    def file_format(el):
        ff = el[0]
        if isinstance(ff, Tuple):
            # TODO: 1 and 2, not 0 and 1?
            result = FileFormat(None, ff[1], ff[2])
        else:
            result = FileFormat(ff)
        return result

    @staticmethod
    def stored(el):
        return el[0]

    @staticmethod
    def primitive_type_tinyint(_):
        return 'int'

    @staticmethod
    def primitive_type_smallint(_):
        return 'int'

    @staticmethod
    def primitive_type_int(_):
        return 'int'

    @staticmethod
    def primitive_type_bigint(_):
        return 'int'

    @staticmethod
    def primitive_type_boolean(_):
        return 'bool'

    @staticmethod
    def primitive_type_float(_):
        return 'float'

    @staticmethod
    def primitive_type_double(_):
        return 'float'

    @staticmethod
    def primitive_type_double_precision(_):
        return 'float'

    @staticmethod
    def primitive_type_string(_):
        return 'str'

    @staticmethod
    def primitive_type_binary(_):
        return 'str'

    @staticmethod
    def primitive_type_timestamp(_):
        return 'str'

    @staticmethod
    def primitive_type_decimal(_):
        return 'str'

    @staticmethod
    def primitive_type_decimal_precision(_):
        return 'str'

    @staticmethod
    def primitive_type_date(_):
        return 'str'

    @staticmethod
    def primitive_type_varchar(_):
        return 'str'

    @staticmethod
    def primitive_type_char(_):
        return 'str'

    @staticmethod
    def primitive_type(el):
        return PrimitiveType(el[0])

    @staticmethod
    def column(el):
        return Column(el[0], el[1])

    @staticmethod
    def columns(el):
        return Columns(el)

    @staticmethod
    def full_column_name(el):
        if len(el) == 3:
            fcn = FullColumnName(*el)
        else:
            fcn = FullColumnName(None, *el)
        return fcn

    @staticmethod
    def table_name(el):
        return TableName(el[0])

    @staticmethod
    def db_name(el):
        return DBName(el[0])

    @staticmethod
    def create_statement(el):
        db_name = DBName('default')
        table_name: Optional[TableName] = None
        columns: Optional[Columns] = None
        file_format: Optional[FileFormat] = None

        for part in el:
            if isinstance(part, DBName):
                db_name = part
            elif isinstance(part, TableName):
                table_name = part
            elif isinstance(part, Columns):
                columns = part
            elif isinstance(part, FileFormat):
                file_format = part
            else:
                LOG.warning('Unsupported type: %s', part)

        return Table(
            db_name=db_name,
            table_name=table_name,
            columns=columns,
            file_format=file_format,
        )


class HiveDDLVisitor(Visitor):
    def __init__(self):
        self.tables: List[Table] = []

    def __default__(self, tree):
        for child in tree.children:
            if isinstance(child, Table):
                self.tables.append(child)


class HiveDDLParser:
    def __init__(self, grammar_file: Union[None, str, PosixPath] = None):
        if grammar_file is None:
            grammar_file = PosixPath(__file__).parent.absolute().parent / 'bnf' / 'hive_ddl.bnf'
        if isinstance(grammar_file, str):
            grammar_file = PosixPath(grammar_file)

        self.grammar_file = grammar_file
        self._grammar: Optional[Lark] = None
        self.tables: Set[Table] = set()

    @property
    def grammar(self):
        if self._grammar is None:
            with self.grammar_file.open('r', encoding='utf8') as fd:
                self._grammar = Lark(fd.read(), parser='lalr')
        return self._grammar

    def parse(self, ddl: str) -> List[Table]:
        tree = self.grammar.parse(ddl)

        transformed_tree = HiveDDLTransformer().transform(tree)

        visitor = HiveDDLVisitor()
        visitor.visit(transformed_tree)

        self.tables.update(visitor.tables)

        return visitor.tables
