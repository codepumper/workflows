from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

SOURCE_REPO = "https://github.com/codepumper/workflows.git"

@task
def create_secrets():
    # env_vars = {
    #     "EODHD_API_KEY": "eodhd-api-key",
    #     "MOTHER_DUCK_TOKEN": "mother-duck-token",
    #     "POLYGON_API_KEY": "polygon-api-key"
    # }

    # for env_var, secret_name in env_vars.items():
    #     value = os.environ.get(env_var)
    #     if value:
    #         Secret(value=value).save(name=secret_name, overwrite=True)
    #     else:
    #         raise ValueError(f"{env_var} environment variable is not set.")
        
    Secret(value='kQcEDfLOTVAT_YYWofBm0HvzRnt1Ujc0').save(name='polygon-api-key', overwrite=True)
    Secret(value='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InJvYmVydGtvdmFjcy5hdEBwcm90b25tYWlsLmNvbSIsInNlc3Npb24iOiJyb2JlcnRrb3ZhY3MuYXQucHJvdG9ubWFpbC5jb20iLCJwYXQiOiJjUjJweWRSRm5OMVh3WEJWTTdLVFQzYzBpUlZyWjZlSGUxRFFHcmh6YjFRIiwidXNlcklkIjoiYTk1NzRjYmEtODYzMC00MTA1LWFjMGItMDQ3NDZlNGZiOGU3IiwiaXNzIjoibWRfcGF0IiwiaWF0IjoxNzI4Njg2MDczfQ.MQ4kAmjR7gdPhsnsooLPiILKjsTjBezlDEbnD9hGoLg').save(name='mother-duck-token', overwrite=True)
    Secret(value='6709872765bcc1.16633535').save(name='eodhd-api-key', overwrite=True)



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
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="polygon_pipeline.py:run_polygon_data_pipeline",
    ).deploy(
        name="polygon_pipeline",
        work_pool_name="data-pipeline-work-pool",
        job_variables={"pip_packages": ["requests", "prefect"]},
        cron="15 0 * * *"
    )

if __name__ == "__main__":
    deploy_eodhd_pipeline()