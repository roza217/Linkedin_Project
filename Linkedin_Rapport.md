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

# 🥉 Phase 1 : Ingestion des Données (Couche BRONZE)

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

## 1. Initialisation de l'Environnement Silver

Avant de procéder au nettoyage, nous isolons les données raffinées dans un schéma dédié. Cette séparation physique est une "best practice" permettant de sécuriser la donnée source (Bronze) et d'organiser la gouvernance des accès.

```sql
-- Initialisation du schéma Silver
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;

-- Vérification de la création
SHOW SCHEMAS IN DATABASE LINKEDIN;
```

## 2. Stratégie Globale de Nettoyage (Phase SILVER)

La transition de la couche **BRONZE** vers la couche **SILVER** ne se limite pas à un simple copier-coller. Elle repose sur une architecture de nettoyage en 4 piliers majeurs pour garantir que chaque donnée est **fiable**, **typée** et **cohérente**.

### 1. Normalisation et Standardisation Textuelle

Les données brutes de LinkedIn contiennent souvent du "bruit" lié à la saisie utilisateur ou au formatage HTML. Pour y remédier, nous appliquons trois traitements systématiques :

* **Suppression des espaces :** Application de la fonction `TRIM()` pour éliminer les espaces blancs superflus avant et après les chaînes de caractères.
* **Traitement des "Faux NULL" :** Utilisation de `NULLIF(col, 'null')` ou `NULLIF(col, '0')`. Dans la couche Bronze, certaines valeurs vides étaient stockées sous forme de texte `'null'` ou `'0'`. Nous les convertissons en véritables valeurs **NULL SQL** pour ne pas fausser les futurs calculs statistiques (moyennes, comptes).
* **Uniformisation de la Casse :** Utilisation de `INITCAP()` pour les noms propres et les spécialités (ex: "SOFTWARE" ➔ "Software") afin d'assurer un rendu visuel professionnel et cohérent dans le dashboard Streamlit.

### 2. Fiabilisation Temporelle (Time Intelligence)

L'un des plus grands défis de ce projet a été la gestion des dates et leur conversion depuis les données sources.

* **Le problème de l'an 1970 :** Les dates LinkedIn sont souvent extraites au format "Unix Epoch" (en millisecondes). Une conversion directe sans vérification préalable générait systématiquement des dates erronées calées sur l'année 1970.
* **La Solution :** Mise en place d'une logique de détection automatique d'unité de temps :
    * Si la valeur est $> 10^{12}$, nous divisons par 1000 pour convertir les millisecondes en secondes.
    * Sinon, la valeur est traitée directement comme des secondes.
* **Résultat :** Toutes les dates de publication (`listed_at`) et d'expiration sont désormais alignées sur le fuseau horaire **NTZ** (No Time Zone) pour garantir une précision totale lors des analyses temporelles.

 ### 3. Sécurisation des Types (Strong Typing)

Pour permettre les analyses avancées de la phase **GOLD**, les données ont été extraites de leur format `STRING` initial vers des types natifs Snowflake :

* **Numérique :** Les salaires (`FLOAT`) et les effectifs (`INT`) ont été convertis pour permettre les agrégations mathématiques ($SUM$, $AVG$).
* **Identifiants :** Les IDs (`job_id`, `company_id`) sont convertis en `BIGINT` pour optimiser les performances des jointures (les jointures sur des entiers sont techniquement plus rapides que sur du texte).
* **Booléens :** Transformation des indicateurs `0/1` ou `'TRUE'/'FALSE'` en véritables types `BOOLEAN`. Cela simplifie l'écriture des filtres dans le dashboard Streamlit (ex: `WHERE is_remote = TRUE`).

### 4. Restructuration du Semi-Structuré (Flattening)

Le projet utilise plusieurs fichiers **JSON** (Companies, Industries, Specialities). La stratégie de nettoyage inclut une étape cruciale de "dépliage" :

* **Usage du `LATERAL FLATTEN` :** Cette fonction Snowflake a été utilisée pour transformer chaque objet à l'intérieur d'un tableau JSON en une ligne relationnelle distincte.
* **Modélisation 1:N :** Une seule ligne Bronze (ex: une entreprise avec une liste de secteurs) est devenue plusieurs lignes Silver (une ligne par secteur d'activité). Cela permet des analyses granulaires par industrie, impossible à réaliser sur un format imbriqué.

## 3. Transformation des 8 Tables Silver

### 3.1 Table `JOB_POSTINGS` (Table Centrale)

**Rôle** : Centralise toutes les offres d'emploi avec leurs attributs principaux.

```sql
-- la Table Job_Postings
-- Transformation de JOBPOSTINGS (Bronze -> Silver)-----------------------------

CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    -- Identifiants (Nettoyés et Castés)
    NULLIF(TRIM(job_id), 'null')::BIGINT AS job_id,
    NULLIF(TRIM(company_name), 'null')::BIGINT AS company_id, 
    
    -- Textes (Tous avec TRIM)
    TRIM(title) AS title,

    -- AJOUT : Détection de la langue basée sur le titre
    CASE 
        WHEN REGEXP_LIKE(TRIM(title), '.*[\\x{4E00}-\\x{9FFF}].*') THEN 'Chinois/Japonais'
        WHEN REGEXP_LIKE(TRIM(title), '.*[\\x{0400}-\\x{04FF}].*') THEN 'Cyrillique'
        WHEN REGEXP_LIKE(TRIM(title), '.*[\\x{0600}-\\x{06FF}].*') THEN 'Arabe'
        WHEN REGEXP_LIKE(TRIM(title), '^[\\x{0000}-\\x{007F}]+$') THEN 'Latin/Anglais'
        ELSE 'Autre / Mixte'
    END AS language_category,

    TRIM(description) AS description,
    TRIM(skills_desc) AS skills_description, 
    TRIM(location) AS location,
    TRIM(posting_domain) AS posting_domain, 
    
    -- Salaire et Compensation
    NULLIF(TRIM(max_salary), 'null')::FLOAT AS max_salary,
    NULLIF(TRIM(med_salary), 'null')::FLOAT AS med_salary,
    NULLIF(TRIM(min_salary), 'null')::FLOAT AS min_salary,
    TRIM(pay_period) AS pay_period,
    TRIM(currency) AS currency,
    TRIM(compensation_type) AS compensation_type,
    
    -- Type de travail
    TRIM(work_type) AS work_type,
    TRIM(formatted_work_type) AS formatted_work_type,
    CASE  
        WHEN TRIM(remote_allowed) = '1.0' THEN TRUE  
        ELSE FALSE  
    END AS is_remote,
    TRIM(formatted_experience_level) AS experience_level,

    -- Statistiques et Candidature
    NULLIF(TRIM(applies), 'null')::INT AS applies_count,
    NULLIF(TRIM(views), 'null')::INT AS views_count,
    TRIM(application_type) AS application_type,
    TRIM(application_url) AS application_url,
    TRIM(job_posting_url) AS job_posting_url,

    -- Dates (Conversion de millisecondes en Timestamp)
    TO_TIMESTAMP_NTZ(NULLIF(TRIM(listed_time), 'null')::BIGINT / 1000) AS listed_at,
    TO_TIMESTAMP_NTZ(NULLIF(TRIM(original_listed_time), 'null')::BIGINT / 1000) AS original_listed_at,
    TO_TIMESTAMP_NTZ(NULLIF(TRIM(expiry), 'null')::BIGINT / 1000) AS expires_at,
    TO_TIMESTAMP_NTZ(NULLIF(TRIM(closed_time), 'null')::BIGINT / 1000) AS closed_at,
    
    -- Booléen
    NULLIF(TRIM(sponsored), 'null')::BOOLEAN AS is_sponsored

FROM LINKEDIN.BRONZE.JOB_POSTINGS;

--Affichage 
SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS ;
```

* Le script nettoie les IDs et les force en format numérique (`BIGINT`).
* **Enrichissement : Détection de la Langue** : Utilisation de `Regex` (expressions régulières) pour identifier les plages de caractères Unicode.

    **Explication** : « Lors de l'exploration initiale des données, nous avons identifié une forte hétérogénéité dans l'origine des offres d'emploi, incluant des titres rédigés en caractères non-latins (Chinois, Japonais, Arabe, Cyrillique) ou composés exclusivement de symboles et de codes numériques. Afin de garantir la pertinence des analyses textuelles ultérieures, nous avons implémenté un script de détection basé sur les plages Unicode via des expressions régulières (Regex). Ce traitement permet d'isoler les titres "Latins/Anglais" des autres systèmes d'écriture et des caractères spéciaux, facilitant ainsi le nettoyage de la colonne et permettant une segmentation précise du marché de l'emploi par zone linguistique. »
  
    **Utilité** : Cela permet de segmenter les offres d'emploi par titre de poste les plus publier sans avoir besoin d'une API externe de traduction.

* Conversion des salaires en `FLOAT` pour permettre des calculs mathématiques (moyennes, médianes).
* **Gestion des Dates (Time Extraction)** : LinkedIn stocke souvent ses dates en **millisecondes** (Epoch Unix).

     **Logique** : On divise par 1000 pour obtenir des secondes, puis on utilise `TO_TIMESTAMP_NTZ` (No Time Zone) pour transformer ce chiffre en une date lisible.

**Note Technique**

Une vérification a été notée concernant le champ company_name source qui semble contenir des identifiants numériques, d'où le cast en BIGINT pour assurer la cohérence des jointures avec la table des entreprises.











