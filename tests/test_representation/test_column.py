from pytest import raises

from spark_ddl_handler.representation.python import Column, PrimitiveType


class TestColumn:
    def test_column_fail(self):
        with raises(TypeError):
            # noinspection PyTypeChecker
            Column('deal_id', 'float')

    def test_column_float(self):
        col = Column('deal_id', PrimitiveType('float'))
        assert str(col) == '    deal_id: float'
