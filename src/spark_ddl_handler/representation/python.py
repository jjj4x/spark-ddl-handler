from dataclasses import dataclass
from typing import Optional, List

INDENT = '    '


@dataclass
class FileFormat:
    standard_format: Optional[str] = None
    custom_input_class: Optional[str] = None
    custom_output_class: Optional[str] = None

    def __str__(self):
        if self.custom_input_class is not None:
            result = f'{self.custom_input_class},{self.custom_output_class}'
        else:
            result = f'{self.standard_format.upper()}'
        return result


@dataclass
class PrimitiveType:
    value: str


@dataclass
class Column:
    column_name: str
    column_type: PrimitiveType

    def __post_init__(self):
        if not isinstance(self.column_type, PrimitiveType):
            raise TypeError

    def __str__(self):
        return f'{INDENT}{self.column_name}: {self.column_type.value}'


@dataclass
class Columns:
    value: List[Column]

    def __str__(self):
        return '\n'.join(
            str(column)
            for column
            in self.value
        )


@dataclass
class FullColumnName:
    db_name: Optional[str]
    table_name: str
    column_name: str

    def __str__(self):
        return '.'.join((
            self.db_name or '',
            self.table_name or '',
            self.column_name,
        ))


@dataclass
class TableName:
    value: str

    def __str__(self):
        return ''.join(
            name.capitalize()
            for name
            in self.value.split('_')
            if name
        )


@dataclass
class DBName:
    value: str

    def __str__(self):
        return self.value


class Table:
    def __init__(
        self,
        db_name: DBName,
        table_name: TableName,
        columns: Columns,
        file_format: FileFormat,
    ):
        self.db_name = db_name
        self.table_name = table_name
        self.columns = columns
        self.file_format = file_format

    def __str__(self):
        lines = '\n'.join((
            'from dataclasses import dataclass',
            '',
            '',
            '@dataclass',
            f'class {self.table_name}:',
            '    class Meta:',
            f'        schema = "{self.db_name}"',
            f'        table = "{self.table_name.value}"',
            f'        stored_as = "{self.file_format}"',
        ))
        return '\n\n'.join((lines, str(self.columns))) + '\n'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)
