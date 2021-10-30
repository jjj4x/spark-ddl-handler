from spark_ddl_handler.representation.python import Columns, Column, PrimitiveType


class TestColumns:
    def test_columns(self):
        cols = Columns([
            Column('deal_id', PrimitiveType('float')),
            Column('balance_mask', PrimitiveType('str')),
        ])
        assert str(cols) == '    deal_id: float\n    balance_mask: str'
