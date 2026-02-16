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

*(Voir le script SQL complet pour les DDL de toutes les tables)*

### Étape 5 & 6 : Chargement et Transformation
*   **CSV** : Chargement direct via `COPY INTO`.
*   **JSON** : Chargement dans les tables RAW, puis `INSERT INTO ... SELECT` avec parsing des champs JSON (`raw_data:key::type`).

Exemple pour `companies` :
```sql
-- Chargement RAW
COPY INTO companies_raw
FROM @linkedin_stage/companies.json
FILE_FORMAT = (FORMAT_NAME = json_format)
ON_ERROR = 'CONTINUE';

-- Transformation et Insertion
INSERT INTO companies
SELECT
  raw_data:company_id::NUMBER,
  raw_data:name::VARCHAR,
  -- ... autres colonnes
  raw_data:url::VARCHAR
FROM companies_raw;
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

## 4. Commentaires
Toutes les étapes ont été scriptées pour être reproductibles. L'architecture choisie sépare clairement les données brutes (Stage/Raw tables) des données structurées (Tables finales), facilitant la maintenance et les évolutions futures.
