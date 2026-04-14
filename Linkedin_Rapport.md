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
