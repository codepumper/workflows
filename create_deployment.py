from prefect import flow

SOURCE_REPO="https://github.com/codepumper/workflows.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="eodhd_pipeline.py:run_eodhd_data_pipeline",
    ).deploy(
        name="eodhd_pipeline",
        work_pool_name="data-pipeline-work-pool",
        cron="5 0 * * *",
    )
