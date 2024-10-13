from prefect import flow, task
from prefect.blocks.system import Secret
import os

SOURCE_REPO = "https://github.com/codepumper/workflows.git"

@task
def create_secrets():
    # Create secrets in Prefect Cloud
    eodhd_api_key = os.environ.get("EODHD_API_KEY")
    mother_duck_token = os.environ.get("MOTHER_DUCK_TOKEN")

    if eodhd_api_key:
        Secret(value=eodhd_api_key).save(name="EODHD_API_KEY", overwrite=True)
    else:
        raise ValueError("EODHD_API_KEY environment variable is not set.")

    if mother_duck_token:
        Secret(value=mother_duck_token).save(name="MOTHER_DUCK_TOKEN", overwrite=True)
    else:
        raise ValueError("MOTHER_DUCK_TOKEN environment variable is not set.")


@flow
def deploy_eodhd_pipeline():

    create_secrets()

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="eodhd_pipeline.py:run_eodhd_data_pipeline",
    ).deploy(
        name="eodhd_pipeline",
        work_pool_name="data-pipeline-work-pool",
        cron="5 5 * * *",
    )

if __name__ == "__main__":
    deploy_eodhd_pipeline()