from spark_ddl_handler.representation.python import FileFormat


class TestFileFormat:
    def test_standard_format(self):
        ff = FileFormat(standard_format='parquet')
        assert str(ff) == 'PARQUET'

    def test_custom_format(self):
        ff = FileFormat(
            custom_input_class='Serializer4000',
            custom_output_class='Deserializer4000',
        )
        assert str(ff) == 'Serializer4000,Deserializer4000'
