from prefect import flow, task
from prefect.blocks.system import Secret
import os

SOURCE_REPO = "https://github.com/codepumper/workflows.git"

@task
def create_secrets():
    env_vars = {
        "EODHD_API_KEY": "eodhd-api-key",
        "MOTHER_DUCK_TOKEN": "mother-duck-token",
        "POLYGON_API_KEY": "polygon-api-key"
    }

    for env_var, secret_name in env_vars.items():
        value = os.environ.get(env_var)
        if value:
            Secret(value=value).save(name=secret_name, overwrite=True)
        else:
            raise ValueError(f"{env_var} environment variable is not set.")


@flow
def deploy_eodhd_pipeline():

    create_secrets()

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="eodhd_pipeline.py:run_eodhd_data_pipeline",
    ).deploy(
        name="eodhd_pipeline",
        work_pool_name="data-pipeline-work-pool",
        job_variables={"pip_packages": ["duckdb==1.1.1", "requests", "pandas", "prefect"]},
        cron="15 0 * * *",
    )

if __name__ == "__main__":
    deploy_eodhd_pipeline()