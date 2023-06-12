
# DW를 이용한 대시보드 구축하기

<img src="https://img.shields.io/badge/github-181717?style=flat&logo=github&logoColor=white"/> <img src="https://img.shields.io/badge/slack-4A154B?style=flat&logo=slack&logoColor=white"/> <img src="https://img.shields.io/badge/googlecloud-4285F4?style=flat&logo=googlecloud&logoColor=white"/> <img src="https://img.shields.io/badge/powerbi-F2C811?style=flat&logo=powerbi&logoColor=white"/>

## 2팀 2조 채용관련 API를 이용한 데이터 직종 일자리 분석

### 팀원 및 역할

    김창민 : 데이터 GCS 적재 및 자동화
    박경모 : 데이터 GCS 적재 및 자동화
    박정우 : 데이터 GCS 적재 및 자동화
    김태준 : GCP 환경 구축 및 자동화 (GCS, workflow, Bigquery)
    임형우 : DB 모델링, 데이터 분석, 대시보드 구축 및 자동화

### 사용 기술
<img src="https://github.com/pjw74/GCP-Job-Board/assets/131341085/57dfd400-4261-400a-bdbf-5d68267af41f" width="300" height="300" />

- - -
### 개요   
사람인에서 재공하는 API와 다른 채용 정보들을 이용하여 현재 데브코스 자체의 의미와 목표인 취업 또는 일자리전망, 유망직종 여부 등의 정보와 추가로 검색량, 자격증 취득 현황 등을 연관관계를 파악할 수 있도록 시각화하여 제공한다.

#### 설정한 지표 목표
- 전체 채용 시장에서 데이터 엔지니어, 데이터 분석가, 데이터 사이언티스트에 대한 채용량의 변화를 시각화
- 각 신입 채용, 채용하는 산업분야, 채용 지역 등의 관점으로 채용량의 변화 파악이 쉽도록 시각화
- 채용정보 외에도 데이터 또는 IT분야에 관련된 구글 검색량, 관련 자격증 응시/취득 여부 데이터를 시간에 따른 시각화

#### 기대 효과
- 시간에 따른 데이터 인력 수요 변화를 이용해 해당 직종에 대한 동향과 이후 전망을 파악할 수 있을 것.
- 원하는 산업분야나 지역에 대해 일자리 공급이 어느정도인지 파악하여 개인의 취업계획에 대해 구체적인 계획을 세우는데 도움을 줄 수 있을 것.
- 검색량에 대한 변화는 개발자가 아닌 일반인들 전체의 관심도를 나타낼 수 있을 것.
- 자격증 데이터는 해당 분야에 대해서 직접적으로 공부를 시작하고, 전문 인력의 수에 대해서 파악할 수 있을 것.
- 이런 [**검색량**, **자격증**, **채용**]. 세 가지 지표를 이용해 연관성을 파악하고 좀 더 유의미한 분석이 가능 할 것으로 예상함.

#### 데이터의 문제점⭐
- 사람인 API를 제외한 다른 API는 개인에게 오픈하지 않거나, 아예 제공되지 않는 경우가 많아 사람인 하나의 데이터로만 진행
- 사람인 채용 데이터의 분포가 17000개 정도의 데이터에서 아래 테이블과 같았으며, 월별로는 23년 5월의 데이터만 12000개로 전체의 70%를 차지하여 시간에 따른 변화를 나타내기에는 부적절한 데이터라고 판단
    - 2023년 데이터로만 한정하여, 모두 현재상태의 지표를 나타내는 것으로 설정함.
    - 이로 인해서 시간에 따른 검색량, 자격증 등의 외부 요인과의 연관성을 도출할 수 없음.

|연도|데이터 수|
|---|---|
|2023|16136|
|2022|937|
|2021|152|
|2020|1|
|2018|1|

- - -
# Workflow
<img src="https://github.com/pjw74/GCP-Job-Board/assets/131341085/5a4b4e90-767a-43dc-94cb-57e90287efcf" width="800" height="500" />

# ETL 과정

## ETL 서버 생성(Google Compute Engine)

### GCP의 VM instance를 활용, ETL 서버를 생성
Setting
- e2-medium
- debian-11-bullseye-v20230509 사용
- linux 환경에서 진행

### Cronjob으로 스케쥴
![Cronjob 세팅](https://github.com/pjw74/GCP-Job-Board/assets/50907018/f1bf7385-0f82-410c-8571-a5acb6b3afa0)

- linux 시스템인 Cronjob을 활용해서 30분 / 1시간마다 ETL 파이썬 스크립트 진행
- 에러 발생 시의 로그를 로컬에 저장하는 식(외부의 DB로 넘길 수 있도록 변경하면 좋을듯)

### 모니터링
- 스크립트 실패 / 데이터 적재 실패 / 인스턴스 다운 에 대해서 크게 3가지로 분류하고 해당 오류에 대한 예외처리 및 SLACK으로 알림 기능 구현

## 채용공고 API ETL
- 실행 시간을 기록하는 파일을 서버에 적재
    - 마지막 스크립트 실행 이후로 업로드된 IT, 개발, 데이터 분야의 채용공고를 수집
    - 예외로 인한 코드 미실행, 특수한 상황으로 인해 이번 시도에 가져오지 못한 데이터 확보 목적
<img src="https://github.com/pjw74/GCP-Job-Board/assets/131341085/d1958e75-e372-4bb9-b1fb-586c60c13161" width="400" height="150" />

### ETL 결과 알림
- Slack inline-hook을 이용해 구현
- 야간시간대에 모집 공고가 거의 없음을 확인
- 지속적인 모니터링으로 업로드 패턴이 정립되면 스케줄링 시간대를 개선하여 비용 측면을 개선 가능할 것
<img src="https://github.com/pjw74/GCP-Job-Board/assets/131341085/b539e9bc-9a8b-455e-bb87-6db72d3a8ba6" width="350" height="200"/>  <img src="https://github.com/pjw74/GCP-Job-Board/assets/131341085/7c9474d4-7570-4650-a3a7-f4d275dd5eca" width="350" height="200" />


## 구글 트렌드 API ETL
- 실행 시간을 기록하는 파일을 서버에 적재
- 예외로 인한 코드 미실행, 구글 트렌드 Requests 과다 요청 등 특수한 상황으로 인해 이번 시도에 가져오지 못한 오류 로그 데이터 확보 목적

<img src="https://github.com/pjw74/GCP-Job-Board/assets/70009161/0de49eb5-f9a4-4868-8b82-0216d2ab97c4" width="350" height="200"/>

### ETL 결과 알림
- Slack inline-hook을 이용해 구현
- 모니터링을 통해 중복된 과거 데이터(2018~2022) 제외하고 최신 구글 트렌드만 업로드하여 비용 측면을 개선 가능할 것

<img src="https://github.com/pjw74/GCP-Job-Board/assets/70009161/ba80d03e-531c-41d2-8739-caa39023cfdf" width="350" height="200"/>

- - - 
# 테이블 모델링
<left>![img1](/query/elt_flow.png)</left>

- **external** : 외부 테이블 스키마   
    (csv파일)
    - recruit_table : csv로 저장된 API응답
    - google_trend : csv로 저장된 구글 트렌드 API응답
    - certification : csv로 저장된 자격증 

- **meta** : 외부테이블 값에 대한 메타데이터   
    (API명세 내용)
    - education : 채용 학력코드 상세
    - experiences : 채용 경력코드 상세
    - industries : 채용 산업코드 상세 (소분류)
    - industry_categories : 채용 산업코드 상세 (대분류)
    - job_codes : 채용 키워드
    - location : 채용 지역 상세

- **raw_data** : 외부 데이터에서 정제/전처리 한 데이터   
    (external.recruit_table로부터 레코드별로 id부여 후 정규화)
    - certification : 자격증 취득 현황 테이블
    - experiences : 경력코드-경력값 테이블
    - google_trend : 구글 트렌드 검색량 테이블
    - industries : 산업코드-대분류-소분류 테이블
    - job_doce_keywords : 키워드코드-키워드 테이블
    - locations : 지역코드-전체지역-상세지역
    - recruits : 공고명, 학력, 급여조건, 게시일자, 조회수, 지원수

- **analystic** : 분석용 요약 테이블   
    (특정 태그에 맞는 직종별 id단일 컬럼으로 구성된 조인테이블로써 구성)
    - da_ids : 데이터 분석가 id목록
    - datajob_ids : de, da, ds 세 직종을 합치고 중복 제거한 id목록
    - de_ids : 데이터 엔지니어 id목록
    - ds_ids : 데이터 사이언티스트 id목록
    - embedded_ids : 임베디드 개발자 id목록
    - security_ids : 보안계열 id목록
    - web_back_ids : 웹 백엔드 id목록
    - web_front_ids : 웹 프론트엔드 id목록

- **visual_tables** : 대시보드 연동 테이블
    - all_count : 전체 recruit 데이터레코드 갯수
    - area_ratio : 전체지역 별 채용 비율
    - area_ratio_seoul : 서울 지역 내 채용 비율
    - certifications : 연간 자격증 응시자/합격자/합격률
    - data_job_detail_ratio : 데이터직종 내 상세 비율
    - data_job_ratio : 전체 채용 중 데이터직종 비율
    - dev_industry_datajobs : 개발자 채용 산업 계 (대분류), (it전체, de, da, ds 별)
    - dev_industry_first_detail : 위 대분류에서 최상위 산업분야의 상세 내용 (it전체, de, da, ds 별)
    - google_trends : 주간 구글 트렌드 검색량
    - recruit_for_exp : 위 analystic에서 분류된 직종 별 전체, 경력, 신입무관, 신입 채용 수
    - top_datajob_keywords : 데이터직종 채용 키워드 TOP30 (de, da, ds 별)
    - top_keywords : IT전체 채용 키워드 TOP50

- **adhoc** : 임시 스키마   
    (쓰이진 않으나 참고용으로 가능하기에 남겨둠)
    - area_ratio_gyeonggi : 경기도 지역 채용 수
    - area_ratio_gyeonggi_sum : 경기도 지역 채용 수 ('구'별 구분 없이)
    - count_of_month : 수집된 데이터 월간 분포 (전처리 이후)
    - count_of_year : 수집된 데이터 연간 분포 (전처리 이후)

- - -
# 대시보드
![img1](/dashboard/w9_visualization_real_last_1.png)
### 전체 채용 비율   
직종 별 공고에 대해서 경력/무관/신입 비율을 알아볼 수 있고,   
데이터 직군에 포커싱을 맞춰서 비율을 알아볼 수 있는 지표
<br><br>
![img2](/dashboard/w9_visualization_real_last_2.png)
### 채용 키워드   
IT전체와 데이터직종에 대한 채용 키워드를 집계를 해 놓아   
어떤 공부가 필요할 지, 어떤 키워드로 채용을 많이 하는지,   
IT전체와 비교하여 어떤 양상을 보이는지 비교할 수 있는 지표
<br><br>
![img3](/dashboard/w9_visualization_real_last_3.png)
### 산업별 채용   
IT기업 외에도 개발자를 채용하는 분야가 어떤 분야가 있는지, 어느정도 있는지를 지표로 시각화
<br><br>
![img4](/dashboard/w9_visualization_real_last_4.png)
### 근무 지역별   
근무지역 분포가 어떻게되는지 파악하기 쉬운 지표를 생성
약 60%정도가 서울에 몰려있기 때문에 서울에 대한 상세 비율 추가.
+경기같은경우 성남에 대부분이 몰려있음. (35~40%)
<br><br>
![img5](/dashboard/w9_visualization_real_last_5.png)
### 자격증 응시   
SQL, ADP, 리눅스마스터에 대한 응시자/합격자 수, 합격률 그래프
정보처리기사(필기) 응시자/합격자 그래프
<br><br>
![img6](/dashboard/w9_visualization_real_last_6.png)
### 트렌드분석   
구글 트렌드API를 이용한 키워드에대한 통계량을 바탕으로 2017년부터 현재까지의 검색량 추이 그래프
(정보처리, 빅데이터, 인공지능, 백준)
