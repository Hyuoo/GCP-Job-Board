-- raw_data 채우는 쿼리셋

-- 1. external.recruit_table로부터 raw_data 아래에 이하 테이블들을 생성
-- industries, locations, job_types, job_code_keywords, experiences
-- recruits
-- 6개 테이블 생성

-- external.recruit_table 로부터 분리할 컬럼 (다중값)
-- position_industry
-- position_location
-- position_job_type
-- position_job_code_keyword_code

-- 다중값분리는 아니지만 경력도 별도의 테이블로 생성함.
-- experiences

-- with origin을 계속 생성하는게 최적화가 될 진 모르겠으나 그냥 만들고 지우는걸로 수정

-- job_types를 따로 생성했었으나, 정규직만 하기로 하여 임시생성해서 모든 테이블에서 정규직인 id만 남기고 job_types는 제거.

DROP TABLE IF EXISTS raw_data.job_types;
DROP TABLE IF EXISTS raw_data.origin;
DROP TABLE IF EXISTS raw_data.industries;
DROP TABLE IF EXISTS raw_data.job_code_keywords;
DROP TABLE IF EXISTS raw_data.locations;
DROP TABLE IF EXISTS raw_data.experiences;
DROP TABLE IF EXISTS raw_data.recruits_n;
DROP TABLE IF EXISTS raw_data.recruits;

DROP TABLE IF EXISTS raw_data.certification;
DROP TABLE IF EXISTS raw_data.google_trend;


-- -- position_job_type 다중값 분리
CREATE TABLE IF NOT EXISTS raw_data.job_types AS 
(
  WITH origin as (SELECT ROW_NUMBER() OVER (ORDER BY posting_date, position_title) id, * FROM external.recruit_table),
  job_code_split as (SELECT id, SPLIT(position_job_type,',') as code_split from origin)

  SELECT DISTINCT id
  FROM job_code_split
  CROSS JOIN UNNEST(job_code_split.code_split) as code
  WHERE code='1'
);


-- 임시 origin 테이블
CREATE TABLE IF NOT EXISTS raw_data.origin AS
(
  SELECT a.*
  FROM (SELECT ROW_NUMBER() OVER (ORDER BY posting_date, position_title) id, * FROM external.recruit_table) as a
  JOIN raw_data.job_types as b
  ON a.id=b.id
);
-- 생성 이후 job_types 필요 없음.
DROP TABLE IF EXISTS raw_data.job_types;


-- -- position_industry 다중값 분리
CREATE TABLE IF NOT EXISTS raw_data.industries AS
(
  WITH job_code_split as (SELECT id, SPLIT(position_industry,',') as code_split from raw_data.origin),
  industries as (
    SELECT
      code,
      industry_category,
      industry
    FROM meta.industries as a
    JOIN meta.industry_categories as b
    ON a.super_code=b.super_code
    )

  SELECT id, a.code, industry_category, industry
  FROM (
    SELECT id, code
    FROM job_code_split
    CROSS JOIN UNNEST(job_code_split.code_split) as code
    ) as a
  JOIN industries as b
  ON a.code=b.code
  GROUP BY 1,2,3,4
);


-- -- position_location 다중값 분리
CREATE TABLE IF NOT EXISTS raw_data.locations AS 
(
  WITH job_code_split as (SELECT id, SPLIT(position_location,',') as code_split from raw_data.origin),
  locations as (
    SELECT
      a.local_code as local_code,
      b.location_name as wide_location,
      a.location_name as location_name
    FROM meta.location as a
    JOIN meta.location as b
    ON a.wide_code=b.local_code
    )

  SELECT id, a.code, b.wide_location, b.location_name
  FROM (
    SELECT id, code
    FROM job_code_split
    CROSS JOIN UNNEST(job_code_split.code_split) as code
    ) as a
  JOIN locations as b
  ON a.code=b.local_code
  GROUP BY 1,2,3,4
);


-- position_job_code_keyword_code
CREATE TABLE IF NOT EXISTS raw_data.job_code_keywords AS
(
  WITH job_code_split as (SELECT id, SPLIT(position_job_code_keyword_code,',') as code_split from raw_data.origin)

  SELECT id, a.code, job_name
  FROM (
    SELECT id, code
    FROM job_code_split
    CROSS JOIN UNNEST(job_code_split.code_split) as code
    ) as a
  JOIN meta.job_codes as b
  ON a.code=b.job_code
  GROUP BY 1,2,3
);


-- experience_table
CREATE TABLE IF NOT EXISTS raw_data.experiences AS 
(
  SELECT 
    a.id as id,
    a.position_experience_level_code as exp_code,
    b.experience as exp_name,
    a.position_experience_level_min as exp_min,
    a.position_experience_level_max as exp_max
  FROM raw_data.origin as a
  JOIN meta.experiences as b
  ON a.position_experience_level_code=b.code
);


-- recruits 테이블 생성
CREATE TABLE IF NOT EXISTS raw_data.recruits_n AS 
(
  SELECT
    id,
    position_title as title,
    position_required_education_level as education_level,
    salary,
    LEFT(posting_date,10) as posting_date,
    LEFT(expiration_date,10) as expiration_date,
    read_cnt,
    apply_cnt
  FROM raw_data.origin
);


-- recruits_2023
-- 2023년 데이터만으로 recruits 재구성. 혹시몰라 위 과정을 남겨놨으나 필요없을듯
CREATE TABLE IF NOT EXISTS raw_data.recruits AS
(
  SELECT id, title, education_level, salary, posting_date, read_cnt, apply_cnt
  FROM raw_data.recruits_n
  WHERE LEFT(posting_date,4)='2023'
);

DROP TABLE IF EXISTS raw_data.origin;
DROP TABLE IF EXISTS raw_data.recruits_n;


-- 자격증 데이터
-- 깨진 컬럼명으로 와서 스키마 생성 후 데이터적재
CREATE TABLE IF NOT EXISTS raw_data.certification(
  test_name STRING,
  test_year INTEGER,
  cert_type STRING,
  take INTEGER,
  pass INTEGER,
  pass_ratio FLOAT64
)
 AS SELECT * FROM external.certification;

-- TREND
-- 깨진 컬럼명으로 와서 스키마 생성 후 데이터적재
CREATE TABLE IF NOT EXISTS raw_data.google_trend(
  date DATE,
  info_proc INTEGER,
  ai INTEGER,
  bigdata INTEGER,
  boj INTEGER,
  boot_camp INTEGER,
  db INTEGER,
  d_mining INTEGER,
  clust INTEGER,
  de INTEGER,
  da INTEGER,
  ds INTEGER
)
 AS SELECT * FROM external.google_trend_V2;
