from spark_ddl_handler.loader.load import Schema, Table

from tests.lib import BaseTestCase


class TestSchema(BaseTestCase):
    schema = 'test_schema'

    def test_show_tables(self):
        schema = Schema(self.schema)

        table_name_rows = schema.show_tables(self.spark)

        table_names = {row.tableName for row in table_name_rows}
        assert 'country_code' in table_names
        assert 'permissions' in table_names

    def test_load_tables(self):
        schema = Schema(self.schema)

        schema.load_tables(self.spark)

        assert len(schema.tables) == 2
        assert isinstance(schema.tables[0], Table)
