from dataclasses import dataclass


@dataclass
class Permissions:
    class Meta:
        schema = "test_schema2"
        table = "permissions"
        stored_as = "PARQUET"

    id: int
    group_name: str
    permission_name: str
