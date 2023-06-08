import pandas as pd
import os
from google.cloud import bigquery
from google.cloud import storage

# project_id = os.environ.get('GCP_PROJECT')
project_id = 'job-posting-api-388303'

def upload_recruit_csv_to_bigquery(data, context):
    # GCS 버킷 내 파일 정보
    bucket_name = data['hale-posting-bucket']
    file_name = data['recruit/recruit_info2023*.csv']
    file_path = f"gs://{bucket_name}:{file_name}"
    
    # 원하는 폴더 경로
    target_folder = 'gs://hale-posting-bucket/recruit/'

    # 해당 폴더 내 파일만 처리
    if not file_name.startswith(target_folder):
        print(f"Ignoring file {file_name}. It is not in the target folder.")
        return
    
    # 빅쿼리 정보
    dataset_id = 'job-posting-api-388303.external'
    table_id = 'job-posting-api-388303.external.recruit_table'

    # 빅쿼리, GCS 클라이언트 생성
    bq_client = bigquery.Client(project=project_id)
    gcs_client = storage.Client(project=project_id)

    df = pd.read_csv(file_path)
    
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        autodetect = True,
        write_disposition = 'WRITE_APPEND'
    )
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"CSV file {file_name} uploaded to BigQuery table {dataset_id}.{table_id}")

# Cloud Function의 엔트리 포인트
def gcs_csv_to_bigquery(event, context):
    # CSV 파일 업로드를 트리거한 GCS 이벤트 정보
    data = event

    # CSV 파일 업로드 처리 함수 호출
    upload_recruit_csv_to_bigquery(data, context)
    return

