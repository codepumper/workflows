from prefect import flow
import os

SOURCE_REPO = "https://github.com/codepumper/workflows.git"

@flow
def deploy_eodhd_pipeline():
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="eodhd_pipeline.py:run_eodhd_data_pipeline",
    ).deploy(
        name="eodhd_pipeline",
        work_pool_name="data-pipeline-work-pool",
        job_variables={
            "env": {
                "EODHD_API_KEY": os.environ.get("EODHD_API_KEY"),
                "MOTHER_DUCK_TOKEN": os.environ.get("MOTHER_DUCK_TOKEN")
            }
        },
        cron="5 5 * * *",
    )

if __name__ == "__main__":
    deploy_eodhd_pipeline()