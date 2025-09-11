from py_load_pmda.schemas import DB_SCHEMA, TABLES_SCHEMA, METADATA_SCHEMA

def test_schemas_are_dicts():
    assert isinstance(DB_SCHEMA, dict)
    assert isinstance(TABLES_SCHEMA, dict)
    assert isinstance(METADATA_SCHEMA, dict)
