from dataclasses import dataclass


@dataclass
class CountryCode:
    class Meta:
        schema = "test_schema2"
        table = "country_code"
        stored_as = "TEXTFILE"

    id: int
    country_name: str
    country_code: str
