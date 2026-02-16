-- 1. Configuration de l'environnement
CREATE DATABASE IF NOT EXISTS LINKEDIN;
USE DATABASE LINKEDIN;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE COMPUTE_WH;

-- 2. Création du Stage Externe
CREATE OR REPLACE STAGE linkedin_stage
  URL = 's3://snowflake-lab-bucket/'
  FILE_FORMAT = (TYPE = 'CSV');

LIST @linkedin_stage;

-- 3. Création des Formats de Fichiers
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  FIELD_DELIMITER = ','
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- 4. Création des Tables
CREATE OR REPLACE TABLE job_postings (
  job_id NUMBER,
  company_name VARCHAR,
  title VARCHAR,
  description VARCHAR(16777216),
  max_salary NUMBER(15,2),
  med_salary NUMBER(15,2),
  min_salary NUMBER(15,2),
  pay_period VARCHAR,
  formatted_work_type VARCHAR,
  location VARCHAR,
  applies NUMBER,
  original_listed_time NUMBER,
  remote_allowed NUMBER,
  views NUMBER,
  job_posting_url VARCHAR,
  application_url VARCHAR(4096),
  application_type VARCHAR,
  expiry NUMBER,
  closed_time NUMBER,
  formatted_experience_level VARCHAR,
  skills_desc VARCHAR(16777216),
  listed_time NUMBER,
  posting_domain VARCHAR,
  sponsored NUMBER,
  work_type VARCHAR,
  currency VARCHAR,
  compensation_type VARCHAR
);

CREATE OR REPLACE TABLE benefits (
  job_id NUMBER,
  inferred BOOLEAN,
  type VARCHAR
);

CREATE OR REPLACE TABLE companies_raw (
  raw_data VARIANT
);

CREATE OR REPLACE TABLE companies (
  company_id NUMBER,
  name VARCHAR,
  description VARCHAR(16777216),
  company_size NUMBER,
  state VARCHAR,
  country VARCHAR,
  city VARCHAR,
  zip_code VARCHAR,
  address VARCHAR,
  url VARCHAR
);

CREATE OR REPLACE TABLE company_industries_raw (
  raw_data VARIANT
);

CREATE OR REPLACE TABLE company_industries (
  company_id NUMBER,
  industry VARCHAR
);

CREATE OR REPLACE TABLE company_specialities_raw (
  raw_data VARIANT
);

CREATE OR REPLACE TABLE company_specialities (
  company_id NUMBER,
  speciality VARCHAR
);

CREATE OR REPLACE TABLE employee_counts (
  company_id NUMBER,
  employee_count NUMBER,
  follower_count NUMBER,
  time_recorded NUMBER
);

CREATE OR REPLACE TABLE job_industries_raw (
  raw_data VARIANT
);

CREATE OR REPLACE TABLE job_industries (
  job_id NUMBER,
  industry_id NUMBER
);

CREATE OR REPLACE TABLE job_skills (
  job_id NUMBER,
  skill_abr VARCHAR
);

-- 5. Chargement des données CSV
INSERT INTO job_postings
SELECT
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
  $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
  $21, $22, $23, $24, $25, $26, $27
FROM @linkedin_stage/job_postings.csv
(FILE_FORMAT => csv_format);

COPY INTO benefits
FROM @linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

COPY INTO employee_counts
FROM @linkedin_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

COPY INTO job_skills
FROM @linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE';

-- 6. Chargement des données JSON (ELT)
COPY INTO companies_raw
FROM @linkedin_stage/companies.json
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'CONTINUE';

INSERT INTO companies
SELECT
  raw_data:company_id::NUMBER,
  raw_data:name::VARCHAR,
  raw_data:description::VARCHAR,
  raw_data:company_size::NUMBER,
  raw_data:state::VARCHAR,
  raw_data:country::VARCHAR,
  raw_data:city::VARCHAR,
  raw_data:zip_code::VARCHAR,
  raw_data:address::VARCHAR,
  raw_data:url::VARCHAR
FROM companies_raw;

COPY INTO company_industries_raw
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'CONTINUE';

INSERT INTO company_industries
SELECT
  raw_data:company_id::NUMBER,
  raw_data:industry::VARCHAR
FROM company_industries_raw;

COPY INTO company_specialities_raw
FROM @linkedin_stage/company_specialities.json
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'CONTINUE';

INSERT INTO company_specialities
SELECT
  raw_data:company_id::NUMBER,
  raw_data:speciality::VARCHAR
FROM company_specialities_raw;

COPY INTO job_industries_raw
FROM @linkedin_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'CONTINUE';

INSERT INTO job_industries
SELECT
  raw_data:job_id::NUMBER,
  raw_data:industry_id::NUMBER
FROM job_industries_raw;

-- 7. Nettoyage
DROP TABLE IF EXISTS companies_raw;
DROP TABLE IF EXISTS company_industries_raw;
DROP TABLE IF EXISTS company_specialities_raw;
DROP TABLE IF EXISTS job_industries_raw;

-- 8. Vérification
SELECT 'job_postings' AS table_name, COUNT(*) AS row_count FROM job_postings
UNION ALL SELECT 'benefits', COUNT(*) FROM benefits
UNION ALL SELECT 'companies', COUNT(*) FROM companies
UNION ALL SELECT 'company_industries', COUNT(*) FROM company_industries
UNION ALL SELECT 'company_specialities', COUNT(*) FROM company_specialities
UNION ALL SELECT 'employee_counts', COUNT(*) FROM employee_counts
UNION ALL SELECT 'job_industries', COUNT(*) FROM job_industries
UNION ALL SELECT 'job_skills', COUNT(*) FROM job_skills;
