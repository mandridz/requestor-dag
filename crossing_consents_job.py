from dagster import get_dagster_logger, job, op
from dagster.utils import file_relative_path
import config as cfg
from dagster_airbyte import airbyte_resource
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_graphql import DagsterGraphQLClient


DBT_PROJECT_DIR = file_relative_path(__file__, cfg.dbt["project_dir"])


crossing_consents = dbt_run_op.alias(name="crossing_consents")


result_dataset = dbt_run_op.alias(name="result_dataset")


@job(
    resource_defs={
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROJECT_DIR,
                "models": ["crossing_consents", "result_dataset"],
            }
        ),
    }
)
def crossing_consents_job():
    logger = get_dagster_logger()
    logger.debug("Starting Crossing Consents Job...")

    result_dataset(start_after=crossing_consents())
