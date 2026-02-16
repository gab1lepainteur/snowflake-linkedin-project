**Participants :** Primael Tchiapo, Nathan Glotin, Enzo Pujol, Gabin Lepainteur, Leuctres Nascimiento

# Projet : Analyse des Offres d'Emploi LinkedIn avec Snowflake

Ce dépôt contient le livrable pour l'évaluation **MBAESG_EVALUATION_ARCHITECTURE_BIGDATA**. Il détaille les étapes de mise en place de l'architecture de données sur Snowflake, le chargement des données, et l'application d'analyse Streamlit.

## 1. Commandes SQL et Explications

### Étape 1 : Configuration de l'environnement
Création de la base de données, du schéma et du warehouse pour le calcul.

```sql
CREATE DATABASE IF NOT EXISTS LINKEDIN;
USE DATABASE LINKEDIN;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE COMPUTE_WH;
```

### Étape 2 : Création du Stage Externe
Connexion au bucket S3 public contenant les données brutes.

```sql
CREATE OR REPLACE STAGE linkedin_stage
  URL = 's3://snowflake-lab-bucket/'
  FILE_FORMAT = (TYPE = 'CSV');
```

### Étape 3 : Création des Formats de Fichiers
Définition des formats pour parser correctement les fichiers CSV et JSON.

```sql
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
```

### Étape 4 : Création des Tables
Création des structures cibles. Pour les fichiers JSON, une approche ELT a été utilisée : chargement d'abord dans une table temporaire (`_RAW`) avec une colonne `VARIANT`, puis transformation vers la table finale.

```sql
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
```

### Étape 5 : Chargement des données CSV
*   **CSV** : Chargement direct via `INSERT INTO ... SELECT FROM`.
*   **JSON** : Chargement dans les tables RAW, puis `COPY INTO ... FROM` avec parsing des champs JSON (`raw_data:key::type`).

Exemple pour `companies` :
```sql
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
```
**ÉTAPE 6 :** Chargement des données JSON (ELT)

```sql
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
```

**ÉTAPE 7 :** Nettoyage
```sql
DROP TABLE IF EXISTS companies_raw;
DROP TABLE IF EXISTS company_industries_raw;
DROP TABLE IF EXISTS company_specialities_raw;
DROP TABLE IF EXISTS job_industries_raw;
```

**ÉTAPE 8 :** Vérification
```sql
SELECT 'job_postings' AS table_name, COUNT(*) AS row_count FROM job_postings
UNION ALL SELECT 'benefits', COUNT(*) FROM benefits
UNION ALL SELECT 'companies', COUNT(*) FROM companies
UNION ALL SELECT 'company_industries', COUNT(*) FROM company_industries
UNION ALL SELECT 'company_specialities', COUNT(*) FROM company_specialities
UNION ALL SELECT 'employee_counts', COUNT(*) FROM employee_counts
UNION ALL SELECT 'job_industries', COUNT(*) FROM job_industries
UNION ALL SELECT 'job_skills', COUNT(*) FROM job_skills;
```
## 2. Code Streamlit

L'application Streamlit permet de visualiser les indicateurs clés. Le code source complet se trouve dans le fichier `streamlit_app.py` de ce dépôt (ou à copier depuis la section dédiée).

**Analyses incluses :**
1.  Top 10 des titres de postes les plus publiés par industrie.
2.  Top 10 des postes les mieux rémunérés par industrie.
3.  Répartition des offres par taille d’entreprise.
4.  Répartition des offres par secteur d’activité.
5.  Répartition des offres par type d’emploi.

## 3. Problèmes Rencontrés et Solutions

### Problème 1 : Incohérence de données dans `job_postings`
*   **Description** : La colonne `company_name` de la table `job_postings` est décrite dans la documentation comme contenant le nom de l'entreprise. Cependant, l'analyse des données a révélé qu'elle contient des **IDs** d'entreprise (ex: '583005.0') au format texte, et non des noms.
*   **Impact** : La jointure initiale `ON jp.company_name = c.name` pour l'analyse par taille d'entreprise ne retournait aucun résultat.
*   **Solution** : Modification de la condition de jointure dans la requête SQL de l'application Streamlit pour utiliser l'ID :
    ```sql
    JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
    ```
    L'utilisation de `TRY_TO_NUMBER` permet de gérer proprement la conversion des chaînes en nombres pour la jointure.

### Problème 2 : Format des fichiers JSON
*   **Description** : Les fichiers JSON contenaient des tableaux d'objets.
*   **Solution** : Utilisation de l'option `STRIP_OUTER_ARRAY = TRUE` dans le `FILE FORMAT` JSON pour permettre à Snowflake de charger chaque objet du tableau comme une ligne distincte.

### Problème 3 : Erreurs de types de colonnes CSV
*   **Description** : Certaines lignes des fichiers CSV pouvaient avoir un nombre de colonnes incohérent ou des caractères mal formés.
*   **Solution** : Utilisation de `ON_ERROR = 'CONTINUE'` lors des commandes `COPY INTO` pour charger toutes les données valides sans bloquer le processus entier sur quelques erreurs mineures de formatage.

### Problème 4 : Noms d'industries manquants
*   **Description** : La table `job_industries` ne contient que des IDs d'industries, et aucune table de mapping (ex: `industries.csv`) n'était fournie pour obtenir les noms.
*   **Solution** : Pour les analyses nécessitant le nom de l'industrie, une jointure a été faite avec la table `company_industries` en passant par la table `companies` (Job -> Company -> Company Industry). Cela suppose que l'industrie de l'offre est celle de l'entreprise, ce qui est une approximation acceptable faute de mieux.
    ```sql
    JOIN companies c ON TRY_TO_NUMBER(jp.company_name) = c.company_id
    JOIN company_industries ci ON c.company_id = ci.company_id
    ```

## 4. Commentaires
Toutes les étapes ont été scriptées pour être reproductibles. L'architecture choisie sépare clairement les données brutes (Stage/Raw tables) des données structurées (Tables finales), facilitant la maintenance et les évolutions futures.
