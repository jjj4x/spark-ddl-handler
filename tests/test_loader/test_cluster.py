from spark_ddl_handler.loader.load import Cluster, Schema, Table

from tests.lib import BaseTestCase, DDL_TEMPLATE_COUNTRY_CODE, DDL_TEMPLATE_PERMISSIONS


class TestCluster(BaseTestCase):
    schema_names = ('test_schema1', 'test_schema2')

    @classmethod
    def setup_class(cls):
        super().setup_class()

        for schema in cls.schema_names:
            cls.recreate_test_schema(schema)
            cls.recreate_test_table(schema, DDL_TEMPLATE_COUNTRY_CODE)
            cls.recreate_test_table(schema, DDL_TEMPLATE_PERMISSIONS)

    def test_load_schemas(self):
        cluster = Cluster(self.schema_names)

        cluster.load_schemas(self.spark)

        assert len(cluster.schemas) == 2
        assert isinstance(cluster.schemas[0], Schema)

        assert len(cluster.schemas[0].tables) == 2
        assert isinstance(cluster.schemas[0].tables[0], Table)

        ddl = cluster.schemas[0].tables[0].ddl
        assert isinstance(ddl, str)
        assert ddl
