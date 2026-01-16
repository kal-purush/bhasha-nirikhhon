from pytest_alembic import tests, create_alembic_fixture

# Due to the alembic config and migrations dir being in a non-standard location,
# an alembic fixture with custom config is generated here:
custom_alembic_fix = create_alembic_fixture({"file": "dbschema/alembic.ini", "script_location":"dbschema/migrations"})

def test_single_head_revision(custom_alembic_fix):
    tests.test_single_head_revision(custom_alembic_fix)

def test_upgrade(custom_alembic_fix):
    tests.test_upgrade(custom_alembic_fix)

def test_model_definitions_match_ddl(custom_alembic_fix):
    tests.test_model_definitions_match_ddl(custom_alembic_fix)

def test_up_down_consistency(custom_alembic_fix):
    tests.test_up_down_consistency(custom_alembic_fix)
