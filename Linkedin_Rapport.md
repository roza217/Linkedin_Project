# 📊 Analyse des Offres d'Emploi LinkedIn avec Snowflake & Streamlit

## 📝 Présentation du Projet
Dans un marché de l'emploi dynamique, l'analyse des données de plateformes comme **LinkedIn** est cruciale pour comprendre les tendances de recrutement. Ce projet met en place un pipeline de données complet, de l'ingestion de données brutes (CSV/JSON) à la visualisation interactive.

L'objectif est de transformer des milliers de lignes de données non traitées en **insights exploitables** pour identifier les secteurs porteurs et les structures de rémunération.

## 🏗️ Architecture Technique : Le Modèle Medallion
J'ai implémenté une architecture **Medallion** pour garantir la qualité et la traçabilité des données :

1.  **Bronze (Raw) :** Ingestion des données brutes depuis un stockage S3 vers Snowflake.
2.  **Silver (Cleaned) :** Nettoyage, dédoublonnage et typage des données.
3.  **Gold (Curated) :** Création de vues agrégées pour l'analyse métier.

---

# 🏗️ Phase 1 : Ingestion des Données (Couche BRONZE)

La couche **Bronze** sert de zone d'atterrissage. L'objectif est de charger les fichiers sources sans transformation pour conserver une "source de vérité".

## 1. Configuration de l'Environnement & Stage
Nous créons d'abord la structure de la base de données et un `STAGE` externe pointant vers le bucket S3 contenant les données.

```sql
-- Création de la base de données et du schéma Bronze
CREATE DATABASE IF NOT EXISTS LINKEDIN;
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;

-- Configuration du Stage pour accéder aux fichiers S3
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
URL = 's3://snowflake-lab-bucket/';
```

## 2. Ingestion des Données Structurées (CSV)

Pour les fichiers au format CSV (Job Postings, Benefits, Employee Counts, Skills), nous avons créé des tables typées `STRING` en couche Bronze. Cela permet d'éviter les erreurs de rejet lors du chargement si certains formats de date ou de nombres sont incohérents.

```sql
-- creation de la table Jobs_posting
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS
(
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    med_salary STRING,
    min_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- charger les donnee dans la table
COPY INTO LINKEDIN.BRONZE.JOBS_POSTING
FROM @LINKEDIN_STAGE/job_postings.csv 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- affichage
select * from LINKEDIN.BRONZE.JOB_POSTINGS;

     --**********--

-- creation de la table benefits
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.benefits (
    job_id string,    
    inferred string,          
    type string        
  );

-- charger les donnee dans la table
COPY INTO LINKEDIN.BRONZE.benefits
FROM @LINKEDIN_STAGE/benefits.csv 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

--affichage
select * from LINKEDIN.BRONZE.benefits;

     --**********--

-- creation de la table employee_counts
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.employee_counts (
    company_id string,        
    employee_count string,               
    follower_count string,               
    time_recorded string        
  );

-- charger les donnee dans la table
COPY INTO LINKEDIN.BRONZE.employee_counts
FROM @LINKEDIN_STAGE/employee_counts.csv 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- affichage
select * from LINKEDIN.BRONZE.employee_counts;

     --**********--

-- creation de la table employee_counts
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.job_skills(
    job_id  string ,       
    skill_abr string        
  );

-- charger les donnee dans la table
COPY INTO LINKEDIN.BRONZE.job_skills
FROM @LINKEDIN_STAGE/job_skills.csv 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- affichage
select * from LINKEDIN.BRONZE.job_skills;
```

* **Note technique :** L'utilisation de `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` est essentielle ici car les descriptions d'emploi sur LinkedIn contiennent souvent des virgules qui pourraient fausser la délimitation des colonnes.
  
## 3. Ingestion des Données Semi-Structurées (JSON)

Pour les fichiers complexes comme `Companies` ou `Industries`, nous avons utilisé le type de données `VARIANT` de Snowflake. C'est un choix stratégique qui permet :
* D'ingérer les fichiers sans définir de schéma à l'avance.
* De conserver l'intégralité des objets JSON pour un parsing ultérieur en couche Silver.

```sql
-- creation de la table Companies 
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.companies (
 data VARIANT                  
);

-- charger les donnee dans la tabl
COPY INTO LINKEDIN.BRONZE.Companies
FROM @LINKEDIN_STAGE/companies.json 
FILE_FORMAT = (TYPE = 'JSON');

-- affichage
select * from LINKEDIN.BRONZE.companies;

     --**********--

-- creation de la table job_industries 
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.job_industries (
 data VARIANT                  
);

-- charger les donnee dans la tabl
COPY INTO LINKEDIN.BRONZE.job_industries
FROM @LINKEDIN_STAGE/job_industries.json
FILE_FORMAT = (TYPE = 'JSON');

-- affichage
select * from LINKEDIN.BRONZE.job_industries;

     --**********--

-- creation de la table LINKEDIN.BRONZE.company_specialities 
CREATE TABLE IF NOT EXISTS company_specialities (
 data VARIANT                  
);

-- charger les donnee dans la tabl
COPY INTO LINKEDIN.BRONZE.company_specialities
FROM @LINKEDIN_STAGE/company_specialities.json
FILE_FORMAT = (TYPE = 'JSON');

-- affichage
select * from LINKEDIN.BRONZE.company_specialities;

     --**********--

-- creation de la table company_industries 
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.company_industries (
 data VARIANT                  
);

-- charger les donnee dans la tabl
COPY INTO LINKEDIN.BRONZE.company_industries
FROM @LINKEDIN_STAGE/company_industries.json
FILE_FORMAT = (TYPE = 'JSON');

-- affichage
select * from LINKEDIN.BRONZE.company_industries;
```

# 🥈 Phase 2 : Raffinement et Normalisation (Couche SILVER)

La couche **Silver** représente l'étape cruciale de transformation structurelle. L'enjeu est de passer d'un stockage brut à un modèle de données **typé**, **propre** et **relationnel**, garantissant l'intégrité des analyses futures.

## 🎯 Stratégie Globale de Nettoyage (Phase SILVER)

La transition de la couche **BRONZE** vers la couche **SILVER** ne se limite pas à un simple copier-coller. Elle repose sur une architecture de nettoyage en 4 piliers majeurs pour garantir que chaque donnée est **fiable**, **typée** et **cohérente**.

### 1. Normalisation et Standardisation Textuelle

Les données brutes de LinkedIn contiennent souvent du "bruit" lié à la saisie utilisateur ou au formatage HTML. Pour y remédier, nous appliquons trois traitements systématiques :

* **Suppression des espaces :** Application de la fonction `TRIM()` pour éliminer les espaces blancs superflus avant et après les chaînes de caractères.
* **Traitement des "Faux NULL" :** Utilisation de `NULLIF(col, 'null')` ou `NULLIF(col, '0')`. Dans la couche Bronze, certaines valeurs vides étaient stockées sous forme de texte `'null'` ou `'0'`. Nous les convertissons en véritables valeurs **NULL SQL** pour ne pas fausser les futurs calculs statistiques (moyennes, comptes).
* **Uniformisation de la Casse :** Utilisation de `INITCAP()` pour les noms propres et les spécialités (ex: "SOFTWARE" ➔ "Software") afin d'assurer un rendu visuel professionnel et cohérent dans le dashboard Streamlit.

