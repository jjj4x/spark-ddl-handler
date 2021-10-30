from spark_ddl_handler.parser.parse import HiveDDLParser


class TestHiveDDLParser:
    country_code = """\
CREATE TABLE `test_table`.`country_code` (
  `id` INT,
  `country_name` STRING,
  `country_code` STRING)
USING text
TBLPROPERTIES (
  'transient_lastDdlTime' = '1634324080')
"""
    permissions = """\
CREATE TABLE `test_table`.`permissions` (
  `id` INT,
  `group_name` STRING,
  `permission_name` STRING)
USING parquet
TBLPROPERTIES (
  'transient_lastDdlTime' = '1634324080')
"""

    def test_can_load_grammar_and_grammar_is_valid(self):
        parser = HiveDDLParser()
        assert parser.grammar

    def test_parsing_country_code(self):
        parser = HiveDDLParser()
        parser.parse(self.country_code)
        parser.parse(self.country_code)

        assert len(parser.tables) == 1

        table = parser.tables.pop()
        assert table.db_name.value == 'test_table'
        assert table.table_name.value == 'country_code'
        assert table.file_format.standard_format == 'TEXTFILE'

        columns = table.columns.value
        assert len(columns) == 3

        assert columns[0].column_type.value == 'int'
        assert columns[1].column_type.value == 'str'
        assert columns[2].column_type.value == 'str'

    def test_parsing_permissions(self):
        parser = HiveDDLParser()
        parser.parse(self.permissions)

        table = parser.tables.pop()
        assert table.db_name.value == 'test_table'
        assert table.table_name.value == 'permissions'
        assert table.file_format.standard_format == 'PARQUET'

        columns = table.columns.value
        assert len(columns) == 3

        assert columns[0].column_type.value == 'int'
        assert columns[1].column_type.value == 'str'
        assert columns[2].column_type.value == 'str'
