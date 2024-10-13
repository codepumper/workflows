from prefect import flow
import os

SOURCE_REPO = "https://github.com/codepumper/workflows.git"

@flow
def deploy_eodhd_pipeline():
    eodhd_api_key = os.getenv('EODHD_API_KEY')
    mother_duck_token = os.getenv('MOTHER_DUCK_TOKEN')
    if not eodhd_api_key or not mother_duck_token:
        raise ValueError("Error: environment variable is not set.")

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="eodhd_pipeline.py:run_eodhd_data_pipeline",
    ).deploy(
        name="eodhd_pipeline",
        work_pool_name="data-pipeline-work-pool",
        job_variables={
            "env":
                "EODHD_API_KEY": eodhd_api_key,
                "MOTHER_DUCK_TOKEN": mother_duck_token
        },
        cron="5 0 * * *",
    )

if __name__ == "__main__":
    deploy_eodhd_pipeline()