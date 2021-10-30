from spark_ddl_handler.representation.python import FileFormat, TableName, Column, PrimitiveType, Columns, Table, \
    DBName


class TestTable:
    def test_simple_table(self):
        db = DBName('mdm')
        tn = TableName('account_balance')
        cols = Columns([
            Column('deal_id', PrimitiveType('float')),
            Column('balance_mask', PrimitiveType('str')),
        ])
        ff = FileFormat(standard_format='parquet')

        table = Table(
            db_name=db,
            table_name=tn,
            columns=cols,
            file_format=ff,
        )

        expected = """\
@dataclass
class AccountBalance:
    class Meta:
        schema = "mdm"
        table = "account_balance"
        stored_as = "PARQUET"

    deal_id: float
    balance_mask: str"""

        assert str(table) == expected
