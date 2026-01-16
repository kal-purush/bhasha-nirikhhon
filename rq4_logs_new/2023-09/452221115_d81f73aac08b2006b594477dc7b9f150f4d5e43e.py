import pytest
import os

import dlt
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.pipeline.utils import airtable_emojis
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration, load_table_counts


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, subset=["duckdb"]), ids=lambda x: x.name)
def test_duck_case_names(destination_config: DestinationTestConfiguration) -> None:
    # we want to have nice tables
    # dlt.config["schema.naming"] = "duck_case"
    os.environ["SCHEMA__NAMING"] = "duck_case"
    pipeline = destination_config.setup_pipeline("test_duck_case_names")
    # create tables and columns with emojis and other special characters
    pipeline.run(airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"))
    pipeline.run([{"🐾Feet": 2, "1+1": "two", "\nhey": "value"}], table_name="🦚Peacocks🦚")
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts == {
        "📆 Schedule": 3,
        "🦚Peacock": 1,
        '🦚Peacock__peacock': 3,
        '🦚Peacocks🦚': 1,
        '🦚WidePeacock': 1,
        '🦚WidePeacock__peacock': 3
    }

    # this will fail - duckdb preserves case but is case insensitive when comparing identifiers
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run([{"🐾Feet": 2, "1+1": "two", "🐾feet": "value"}], table_name="🦚peacocks🦚")
    assert isinstance(pip_ex.value.__context__, DatabaseTerminalException)

    # show tables and columns
    with pipeline.sql_client() as client:
        with client.execute_query("DESCRIBE 🦚peacocks🦚;") as q:
            tables = q.df()
    assert tables["column_name"].tolist() == ["🐾Feet", "1+1", "hey", "_dlt_load_id", "_dlt_id"]

