

# Role & Purpose 
: ETL 과정으로 GCS에 주기적으로 적재되는 데이터를 Bigquery로 스케줄링하여 자동화

# Working 
1. GCS 및 Bigquery 생성 후 IAM 부여 및 테스트 진행
2. GCSDP 주기적으로 적재되는 데이터에 한해 cloud function 코드 생성
3. cloud scheduler로 function 주기적으로 실행 (프로젝트 기간 매일 자정에 작업 진행)
4. GCP 내 workflow활용해 flow 작업 체크 
5. 작업 실시할 때마다 slack, email로 체크
-> 클라우드 기반 GCP의 전반적인 기능을 활용하여 데이터 파이프라인 구축

# 프로젝트 진행 시 학습한 내용
- 데이터를 바로 Bigquery에 적재하지 않고, GCS를 거쳐 적재되도록 자동화 
-> 추후 AWS S3, Dropbox 등 클라우드 스토리지 기반 서비스의 확장성 고려
- GCP 내 콘솔 가이드가 상당히 잘되어있어 UI 측면에서 사용 편리

# 프로젝트 진행 시 어려웠던 부분
- GCS -> Bigquery 자동화 과정에서 cloud function에 있어 표본이 적어 코드 작성에 시간이 꽤 소요
- IAM 부여가 프로젝트 단위로만 진행되어 초기 계획한 내용 변경 (초기 계획 : GCS, DW 개별 관리)
- Airflow 등의 타 ETL과 연동하는 부분, Redshift 등 타 DB 혹은 타 CS와의 연동 과정에 있어 세부적인 추가 학습 필요
