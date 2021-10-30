from unittest.mock import MagicMock

from spark_ddl_handler.loader.load import Table

from tests.lib import BaseTestCase


class TestTable(BaseTestCase):
    schema = 'test_table'

    def setup_method(self):
        self.table_fixture = Table('raw', 'prod_iam_permissions')

    def test_name(self):
        assert self.table_fixture.name == 'raw.prod_iam_permissions'

    def test_load_ddl_returns_self(self):
        spark_mock = MagicMock()
        assert isinstance(self.table_fixture.load_ddl(spark_mock), Table)

    def test_integrated_load_ddl(self):
        schema = self.schema

        table_country_code = Table(schema, 'country_code').load_ddl(self.spark)
        table_permissions = Table(schema, 'permissions').load_ddl(self.spark)

        assert table_country_code.ddl
        assert table_permissions.ddl

        assert f'CREATE TABLE `{self.schema}`.`country_code` (' in table_country_code.ddl
        assert 'USING text' in table_country_code.ddl
        assert 'USING parquet' in table_permissions.ddl
