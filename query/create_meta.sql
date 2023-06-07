DROP TABLE IF EXISTS external.education;
DROP TABLE IF EXISTS external.experiences;
DROP TABLE IF EXISTS external.industry;
DROP TABLE IF EXISTS external.industry_categories;
DROP TABLE IF EXISTS external.job_codes;
DROP TABLE IF EXISTS external.job_types;
DROP TABLE IF EXISTS external.location;
DROP TABLE IF EXISTS external.salaries;

-- 코드로 전처리한 csv파일로부터 각 메타데이터 테이블 생성
-- Storage에서 외부테이블 생성 후, meta로 복사 후 삭제

LOAD DATA OVERWRITE external.education (
  code string,
  education_level string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/education.csv']
);
CREATE OR REPLACE TABLE meta.education AS SELECT * FROM external.education;
DROP TABLE IF EXISTS external.education;


LOAD DATA OVERWRITE external.experiences (
  code string,
  experience string,
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/experience.csv']
);
CREATE OR REPLACE TABLE meta.experiences AS SELECT * FROM external.experiences;
DROP TABLE IF EXISTS external.experiences;


LOAD DATA OVERWRITE external.industries (
  code string,
  industry string,
  super_code string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/industry.csv']
);
CREATE OR REPLACE TABLE meta.industries AS SELECT * FROM external.industries;
DROP TABLE IF EXISTS external.industries;


LOAD DATA OVERWRITE external.industry_categories (
  super_code string,
  industry_category string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/industry_category.csv']
);
CREATE OR REPLACE TABLE meta.industry_categories AS SELECT * FROM external.industry_categories;
DROP TABLE IF EXISTS external.industry_categories;


LOAD DATA OVERWRITE external.job_codes (
  super_code string,
  super_name string,
  job_code string,
  job_name string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/job_code.csv']
);
CREATE OR REPLACE TABLE meta.job_codes AS SELECT job_code, job_name FROM external.job_codes;
DROP TABLE IF EXISTS external.job_codes;


LOAD DATA OVERWRITE external.job_types (
  code string,
  job_type string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/job_type.csv']
);
CREATE OR REPLACE TABLE meta.job_types AS SELECT * FROM external.job_types;
DROP TABLE IF EXISTS external.job_types;


LOAD DATA OVERWRITE external.location (
  not_use string,
  location_name string,
  local_code string,
  wide_code string
)
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://hale-posting-bucket/recruit_meta/location.csv']
);
CREATE OR REPLACE TABLE meta.location AS SELECT wide_code, local_code, location_name FROM external.location;
DROP TABLE IF EXISTS external.location;


-- LOAD DATA OVERWRITE external.salaries (
--   code string,
--   salary string
-- )
-- FROM FILES (
--   format = 'CSV',
--   skip_leading_rows = 1,
--   uris = ['gs://hale-posting-bucket/recruit_meta/salary.csv']
-- );
-- CREATE OR REPLACE TABLE meta.salaries AS SELECT * FROM external.salaries;
DROP TABLE IF EXISTS external.salaries;
