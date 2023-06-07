-- BI툴과 연동될 테이블 생성
-- 총 생성 테이블 12개

DROP TABLE IF EXISTS visual_tables.all_count;
DROP TABLE IF EXISTS visual_tables.recruit_for_exp;
-- DROP TABLE IF EXISTS visual_tables.top_de_keywords;
-- DROP TABLE IF EXISTS visual_tables.top_da_keywords;
-- DROP TABLE IF EXISTS visual_tables.top_ds_keywords;
DROP TABLE IF EXISTS visual_tables.top_datajob_keywords;
DROP TABLE IF EXISTS visual_tables.top_keywords;
DROP TABLE IF EXISTS visual_tables.data_job_ratio;
DROP TABLE IF EXISTS visual_tables.data_job_detail_ratio;
DROP TABLE IF EXISTS visual_tables.area_ratio;
DROP TABLE IF EXISTS visual_tables.area_ratio_seoul;
-- DROP TABLE IF EXISTS visual_tables.dev_industry_all;
-- DROP TABLE IF EXISTS visual_tables.dev_industry_datajob;
-- DROP TABLE IF EXISTS visual_tables.dev_industry_de;
-- DROP TABLE IF EXISTS visual_tables.dev_industry_da;
-- DROP TABLE IF EXISTS visual_tables.dev_industry_ds;
DROP TABLE IF EXISTS visual_tables.dev_industry_datajobs;
DROP TABLE IF EXISTS visual_tables.dev_industry_first_detail;
DROP TABLE IF EXISTS visual_tables.certifications;
DROP TABLE IF EXISTS visual_tables.google_trends;


CREATE TABLE IF NOT EXISTS visual_tables.all_count AS (
  SELECT COUNT(*) as `IT전체` FROM raw_data.recruits
);

-- 직종 별 전체모집, 경력, 신입무관, 신입 공고 수
CREATE TABLE IF NOT EXISTS visual_tables.recruit_for_exp AS(
  WITH tmp AS(
    SELECT
      ta.id as a, tb.id as b, tc.id as c, td.id as d, te.id as e, tf.id as f, tg.id as g, th.id as h, ti.id as i,
      exp_code, exp_name, exp_min
    FROM `raw_data.experiences` as ta
    LEFT JOIN `analystic.datajob_ids` as tb
    USING (id)
    LEFT JOIN `analystic.de_ids` as tc
    USING (id)
    LEFT JOIN `analystic.da_ids` as td
    USING (id)
    LEFT JOIN `analystic.ds_ids` as te
    USING (id)
    LEFT JOIN `analystic.web_front_ids` as tf
    USING (id)
    LEFT JOIN `analystic.web_back_ids` as tg
    USING (id)
    LEFT JOIN `analystic.embedded_ids` as th
    USING (id)
    LEFT JOIN `analystic.security_ids` as ti
    USING (id)
    )

  SELECT s as `분류`, a as `전체`, b as `경력`, c as `신입 무관`,d as `신입` FROM (
    -- (
    --   SELECT
    --     'IT전체' as s,
    --     count(*) as a,
    --     count(CASE WHEN exp_code='2' THEN 1 END) as b,
    --     count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
    --     count(CASE WHEN exp_code='1' THEN 1 END) as d
    --   FROM tmp
    -- )
    -- UNION ALL
    (
      SELECT
        '데이터직종' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE b IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '데이터엔지니어' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE c IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '데이터분석가' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE d IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '데이터과학자' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE e IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '프론트엔드 개발자' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE f IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '백엔드 개발자' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE g IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '임베디드 개발자' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE h IS NOT NULL
    )
    UNION ALL
    (
      SELECT
        '정보보안' as s,
        count(*) as a,
        count(CASE WHEN exp_code='2' THEN 1 END) as b,
        count(CASE WHEN CAST(exp_code as INTEGER) IN (1,3,4) AND exp_min=0 THEN 1 END) as c,
        count(CASE WHEN exp_code='1' THEN 1 END) as d
      FROM tmp WHERE i IS NOT NULL
    )
  )
  ORDER BY 2 DESC
);
  -- 행-열 반전X 코드
  -- SELECT s as `분류`, a as `IT전체`, b as `데이터직종`,c as `데이터엔지니어` FROM (
  --   (SELECT '전체' as s, count(a) as a, count(b) as b, count(c) as c FROM tmp)
  --   UNION ALL
  --   (SELECT '신입/무관' as s, count(a) as a, count(b) as b, count(c) as c FROM tmp WHERE CAST(exp_code as integer) IN (1,3,4) AND exp_min=0)
  --   UNION ALL
  --   (SELECT '신입' as s, count(a) as a, count(b) as b, count(c) as c FROM tmp WHERE exp_code='1')
  -- )


-- -- 데이터엔지니어 공고들의 연관 키워드들 TOP 30
-- CREATE TABLE IF NOT EXISTS visual_tables.top_de_keywords AS(
--   SELECT job_name, count(*) as count
--   FROM raw_data.job_code_keywords as a
--   JOIN analystic.de_ids as b
--   ON a.id=b.id
--   GROUP BY job_name
--   ORDER BY count DESC
--   LIMIT 30
-- );
-- -- 데이터분석가 공고들의 연관 키워드들 TOP 30
-- CREATE TABLE IF NOT EXISTS visual_tables.top_da_keywords AS(
--   SELECT job_name, count(*) as count
--   FROM raw_data.job_code_keywords as a
--   JOIN analystic.da_ids as b
--   ON a.id=b.id
--   GROUP BY job_name
--   ORDER BY count DESC
--   LIMIT 30
-- );
-- -- 데이터과학자 공고들의 연관 키워드들 TOP 30
-- CREATE TABLE IF NOT EXISTS visual_tables.top_ds_keywords AS(
--   SELECT job_name, count(*) as count
--   FROM raw_data.job_code_keywords as a
--   JOIN analystic.ds_ids as b
--   ON a.id=b.id
--   GROUP BY job_name
--   ORDER BY count DESC
--   LIMIT 30
-- );


-- 데이터 직무 전체에서의 연관 키워드들 TOP 30 으로 뽑은 각 분야별 카운트
CREATE TABLE IF NOT EXISTS visual_tables.top_datajob_keywords AS(
  WITH
    datajob_keywords AS
    (
      SELECT job_name, de.id as de, da.id as da, ds.id as ds
      FROM raw_data.job_code_keywords as a
      JOIN analystic.datajob_ids as b
      ON a.id=b.id
      LEFT JOIN analystic.de_ids as de
      ON a.id=de.id
      LEFT JOIN analystic.da_ids as da
      ON a.id=da.id
      LEFT JOIN analystic.ds_ids as ds
      ON a.id=ds.id
    ),
    keywords AS
    (
      SELECT job_name, count(*) as count
      FROM raw_data.job_code_keywords as a
      JOIN analystic.datajob_ids as b
      USING (id)
      GROUP BY job_name
      ORDER BY count DESC
      LIMIT 30
    )
  
  SELECT
    a.job_name as job_name,
    COUNT(CASE WHEN de IS NOT NULL THEN 1 END) as de,
    COUNT(CASE WHEN da IS NOT NULL THEN 1 END) as da,
    COUNT(CASE WHEN ds IS NOT NULL THEN 1 END) as ds
  FROM datajob_keywords as a
  JOIN keywords as b
  ON a.job_name=b.job_name
  GROUP BY job_name
);



-- 모든 공고에서 가장 많은 직무 키워드 TOP 50
CREATE TABLE IF NOT EXISTS visual_tables.top_keywords AS(
  SELECT job_name, count(DISTINCT id) as count
  FROM raw_data.job_code_keywords
  GROUP BY job_name
  ORDER BY 2 DESC
  LIMIT 50
);


-- 전체 중 데이터 직군 비율
CREATE TABLE IF NOT EXISTS visual_tables.data_job_ratio AS(
  SELECT (CASE WHEN b.id is not null THEN '데이터직군' ELSE '데이터직군 외' END) as `분류`, count(*) as count
  FROM raw_data.recruits as a
  LEFT JOIN analystic.datajob_ids as b
  ON a.id=b.id
  GROUP BY 1
);

-- 데이터 직군별 비율
CREATE TABLE IF NOT EXISTS visual_tables.data_job_detail_ratio AS(
  SELECT a as `분류`,b as `공고수` FROM (
    (SELECT '데이터엔지니어' as a, COUNT(*) as b FROM analystic.de_ids)
    UNION ALL
    (SELECT '데이터분석가' as a, COUNT(*) as b FROM analystic.da_ids)
    UNION ALL
    (SELECT '데이터사이언티스트' as a, COUNT(*) as b FROM analystic.ds_ids)
  )
);


-- 근무 지역 전체 비율
CREATE TABLE IF NOT EXISTS visual_tables.area_ratio AS
(
  SELECT wide_location, count(a.id) as it, count(b.id) as datajob
  FROM raw_data.locations as a
  LEFT JOIN analystic.datajob_ids as b
  ON a.id=b.id
  WHERE CAST(code as integer)<200000
  GROUP BY wide_location
  ORDER BY 2 DESC
);


-- 서울 근무 지역 비율
CREATE TABLE IF NOT EXISTS visual_tables.area_ratio_seoul AS
(
  SELECT location_name, count(a.id) as it, count(b.id) as datajob
  FROM raw_data.locations as a
  LEFT JOIN analystic.datajob_ids as b
  ON a.id=b.id
  WHERE wide_location LIKE '서울%'
  AND location_name NOT LIKE '서울%'
  GROUP BY location_name
  ORDER BY 2 DESC
);

-- -- 경기 근무 지역 비율
-- CREATE TABLE IF NOT EXISTS adhoc.area_ratio_gyeonggi AS
-- (
--   SELECT location_name, count(a.id) as it, count(b.id) as datajob
--   FROM raw_data.locations as a
--   LEFT JOIN analystic.datajob_ids as b
--   ON a.id=b.id
--   WHERE wide_location LIKE '경기%'
--   AND location_name NOT LIKE '경기%'
--   GROUP BY location_name
--   ORDER BY 2 DESC
-- );
-- 경기 지역 상세'구' 구분X 합
-- CREATE TABLE IF NOT EXISTS adhoc.area_ratio_gyeonggi_sum AS
-- (
--   SELECT LEFT(location_name, 3) as name, count(a.id) as it, count(b.id) as datajob
--   FROM raw_data.locations as a
--   LEFT JOIN analystic.datajob_ids as b
--   ON a.id=b.id
--   WHERE wide_location LIKE '경기%'
--   AND location_name NOT LIKE '경기%'
--   GROUP BY name
--   ORDER BY 2 DESC
-- )

-- -- 전체 IT인력 모집 업종 비율
-- CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_all AS
-- (
--   SELECT industry_category, industry, count(*) as count
--   FROM raw_data.industries
--   GROUP BY industry_category, industry
--   ORDER BY 3 DESC
-- );

-- -- 데이터 인력 모집 업종 비율
-- CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_datajob AS
-- (
--   SELECT industry_category, industry, count(*) as count
--   FROM raw_data.industries as a
--   JOIN analystic.datajob_ids as b
--   ON a.id=b.id
--   GROUP BY industry_category, industry
--   ORDER BY 3 DESC
-- );

-- -- 데이터 엔지니어 모집 업종 비율
-- CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_de AS
-- (
--   SELECT industry_category, industry, count(*) as count
--   FROM raw_data.industries as a
--   JOIN analystic.de_ids as b
--   ON a.id=b.id
--   GROUP BY industry_category, industry
--   ORDER BY 3 DESC
-- );

-- -- 데이터 분석가 모집 업종 비율
-- CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_da AS
-- (
--   SELECT industry_category, industry, count(*) as count
--   FROM raw_data.industries as a
--   JOIN analystic.da_ids as b
--   ON a.id=b.id
--   GROUP BY industry_category, industry
--   ORDER BY 3 DESC
-- );

-- -- 데이터 과학자 모집 업종 비율
-- CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_ds AS
-- (
--   SELECT industry_category, industry, count(*) as count
--   FROM raw_data.industries as a
--   JOIN analystic.ds_ids as b
--   ON a.id=b.id
--   GROUP BY industry_category, industry
--   ORDER BY 3 DESC
-- );

-- 모든 데이터직종 모집 업종 비율
CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_datajobs AS(
  WITH
    datajob_industries AS
    (
      SELECT industry_category, industry, a.id as it, de.id as de, da.id as da, ds.id as ds
      FROM raw_data.industries as a
      LEFT JOIN analystic.datajob_ids as b
      ON a.id=b.id
      LEFT JOIN analystic.de_ids as de
      ON a.id=de.id
      LEFT JOIN analystic.da_ids as da
      ON a.id=da.id
      LEFT JOIN analystic.ds_ids as ds
      ON a.id=ds.id
    )
  
  SELECT
    industry_category, industry,
    COUNT(it) as it,
    COUNT(de) as de,
    COUNT(da) as da,
    COUNT(ds) as ds
  FROM datajob_industries
  GROUP BY industry_category, industry
);

-- 전체의 [IT,웹,통신]카테고리 상세 업종 비율
CREATE TABLE IF NOT EXISTS visual_tables.dev_industry_first_detail AS(
  WITH
    datajob_industries AS
    (
      SELECT industry, a.id as it, de.id as de, da.id as da, ds.id as ds
      FROM (
        SELECT id, industry FROM raw_data.industries WHERE industry_category LIKE 
        (
          SELECT industry_category FROM raw_data.industries GROUP BY industry_category ORDER BY count(*) DESC LIMIT 1
        )
      ) as a
      LEFT JOIN analystic.datajob_ids as b
      ON a.id=b.id
      LEFT JOIN analystic.de_ids as de
      ON a.id=de.id
      LEFT JOIN analystic.da_ids as da
      ON a.id=da.id
      LEFT JOIN analystic.ds_ids as ds
      ON a.id=ds.id
    )
  
  SELECT
    industry,
    COUNT(it) as it,
    COUNT(de) as de,
    COUNT(da) as da,
    COUNT(ds) as ds
  FROM datajob_industries
  GROUP BY industry
);


-- 자격증 취득 현황
CREATE TABLE IF NOT EXISTS visual_tables.certifications AS
(
  WITH
    a AS (
    SELECT DISTINCT test_year as year
    FROM raw_data.certification
    ),
    b AS (
      SELECT * FROM a
      LEFT JOIN (
        SELECT test_year as year, take as `정보처리기사 응시자`, pass as `정보처리기사 합격자`, ROUND((pass/take)*100,2) as `정보처리기사 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE '정보%'
      )
      USING (year)
    ),
    c AS (
      SELECT * FROM b
      LEFT JOIN (
        SELECT test_year as year, take as `SQLD 응시자`, pass as `SQLD 합격자`, ROUND((pass/take)*100,2) as `SQLD 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE 'sql' AND cert_type LIKE '개발자%'
      )
      USING (year)
    ),
    d AS (
      SELECT * FROM c
      LEFT JOIN (
        SELECT test_year as year, take as `SQLP 응시자`, pass as `SQLP 합격자`, ROUND((pass/take)*100,2) as `SQLP 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE 'sql' AND cert_type LIKE '전문가%'
      )
      USING (year)
    ),
    e AS (
      SELECT * FROM d
      LEFT JOIN (
        SELECT test_year as year, take as `ADP 응시자`, pass as `ADP 합격자`, ROUND((pass/take)*100,2) as `ADP 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE '데이터%' AND cert_type LIKE '전문가%'
      )
      USING (year)
    ),
    f AS (
      SELECT * FROM e
      LEFT JOIN (
        SELECT test_year as year, take as `ADsP 응시자`, pass as `ADsP 합격자`, ROUND((pass/take)*100,2) as `ADsP 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE '데이터%' AND cert_type LIKE '준전문가%'
      )
      USING (year)
    ),
    g AS (
      SELECT * FROM f
      LEFT JOIN (
        SELECT test_year as year, take as `리눅스마스터 1급 응시자`, pass as `리눅스마스터 1급 합격자`, ROUND((pass/take)*100,2) as `리눅스마스터 1급 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE "리눅스%" AND cert_type LIKE '1급%'
      )
      USING (year)
    ),
    h AS (
      SELECT * FROM g
      LEFT JOIN (
        SELECT test_year as year, take as `리눅스마스터 2급 응시자`, pass as `리눅스마스터 2급 합격자`, ROUND((pass/take)*100,2) as `리눅스마스터 2급 합격률`
        FROM raw_data.certification
        WHERE test_name LIKE "리눅스%" AND cert_type LIKE '2급%'
      )
      USING (year)
    )
  SELECT *
  FROM h
  ORDER BY year DESC
);

-- 트렌드 그래프
CREATE TABLE IF NOT EXISTS visual_tables.google_trends AS
(
  SELECT date, info_proc, ai, bigdata, boj --, de, da, ds
  FROM raw_data.google_trend
  ORDER BY 1
);
