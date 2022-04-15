from dagster import get_dagster_logger, job, op
from dagster.utils import file_relative_path
import config as cfg
from dagster_airbyte import airbyte_resource, airbyte_sync_op
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_graphql import DagsterGraphQLClient

sync_requestor = airbyte_sync_op.configured(
    {"connection_id": cfg.db_source_requestor["connection_id"]}, name="sync_requestor"
)

sync_provider1_base_consents = airbyte_sync_op.configured(
    {"connection_id": cfg.db_source_provider1["connection_id"]},
    name="sync_provider1_base_consents",
)

DBT_PROJECT_DIR = file_relative_path(__file__, cfg.dbt["project_dir"])

base_consents = dbt_run_op.alias(name="base_consents")


@op
def exec_provider1_base_consents_job(something):
    client = DagsterGraphQLClient(
        hostname=cfg.dagster_provider1["hostname"],
        port_number=cfg.dagster_provider1["port_number"],
    )
    client.submit_job_execution("crossing_datasets_job")


@job(
    resource_defs={
        "airbyte": airbyte_resource.configured(
            {
                "host": cfg.airbyte["host"],
                "port": cfg.airbyte["port"],
            }
        ),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROJECT_DIR,
                "models": ["base_consents"],
            }
        ),
    }
)
def base_consents_job():
    logger = get_dagster_logger()
    logger.debug("Starting Job...")

    exec_provider1_base_consents_job(
        sync_provider1_base_consents(
            start_after=base_consents(start_after=sync_requestor())
        )
    )
