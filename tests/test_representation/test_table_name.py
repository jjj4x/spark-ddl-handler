from spark_ddl_handler.representation.python import TableName


class TestTableName:
    def test_table_name_typical(self):
        tn = TableName('account_balance')
        assert str(tn) == 'AccountBalance'

    def test_table_name_strange(self):
        tn = TableName('_tech__columns_all_')
        assert str(tn) == 'TechColumnsAll'
