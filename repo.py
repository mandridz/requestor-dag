from dagster import repository
from base_consents_job import base_consents_job
from crossing_consents_job import crossing_consents_job


@repository
def requestor_repository():
    return [base_consents_job, crossing_consents_job]
