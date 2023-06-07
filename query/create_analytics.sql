-- 직무 키워드, 모집제목에서 키워드 일치하는 레코드들 분류
-- 조인테이블용 id 단일컬럼

DROP TABLE IF EXISTS analystic.de_ids;
DROP TABLE IF EXISTS analystic.da_ids;
DROP TABLE IF EXISTS analystic.ds_ids;
DROP TABLE IF EXISTS analystic.datajob_ids;
DROP TABLE IF EXISTS analystic.embedded_ids;
DROP TABLE IF EXISTS analystic.web_front_ids;
DROP TABLE IF EXISTS analystic.web_back_ids;
DROP TABLE IF EXISTS analystic.security_ids;


-- 데이터 엔지니어 IDs
CREATE TABLE IF NOT EXISTS analystic.de_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE (
    job_name LIKE '데이터엔지니어'
    OR title LIKE '%데이터_엔지니어%'
  )
);

-- 데이터 분석가 IDs
CREATE TABLE IF NOT EXISTS analystic.da_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE (
    job_name LIKE '데이터분석가'
    OR job_name LIKE '데이터시각화'
    OR title LIKE '%데이터분석%'
    OR title LIKE '%데이터 분석%'
  )
);

-- 데이터 과학자 IDs
CREATE TABLE IF NOT EXISTS analystic.ds_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE (
    job_name LIKE '%러닝'
    OR job_name LIKE '%인공%'
    OR title LIKE '%머신러닝%' -- 이러닝이 껴있길래 상세히 구분
    OR title LIKE '%딥러닝%'
    OR title LIKE '%데이터사이%'
    OR title LIKE '%데이터 사이%'
    OR title LIKE '%데이터과학%'
    OR title LIKE '%데이터 과학%'
  )
);

-- 데이터 직종 IDs
-- 위 세 테이블 합
CREATE TABLE IF NOT EXISTS analystic.datajob_ids AS (
  SELECT DISTINCT id
  FROM (
    (SELECT * FROM analystic.de_ids)
    UNION ALL
    (SELECT * FROM analystic.da_ids)
    UNION ALL
    (SELECT * FROM analystic.ds_ids)
  )
);

-- 웹 프론트 IDs
CREATE TABLE IF NOT EXISTS analystic.web_front_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE (
    job_name LIKE '프론트%'
  )
);

-- 웹 백엔드 IDs
CREATE TABLE IF NOT EXISTS analystic.web_back_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE job_name LIKE '백엔드%'
);

-- 임베디드 IDs
CREATE TABLE IF NOT EXISTS analystic.embedded_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE (
    job_name LIKE '임베디드'
    OR job_name LIKE '펌웨어'
  )
);

-- 정보보안 IDs
CREATE TABLE IF NOT EXISTS analystic.security_ids AS (
  SELECT DISTINCT a.id
  FROM raw_data.recruits as a
  JOIN raw_data.job_code_keywords as b
  ON a.id=b.id
  WHERE job_name LIKE '%보안%'
);
