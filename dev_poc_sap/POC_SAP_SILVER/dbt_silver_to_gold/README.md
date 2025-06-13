# dbt_silver_to_gold

## Vue d'ensemble

Ce projet dbt fait partie du Proof of Concept visant à accélérer le Time-To-Value des données SAP en utilisant Snowflake et dbt.  
Auteur : **M'HAMED ISSAM ED-DAOU**

Le dossier `dbt_silver_to_gold` contient les transformations nécessaires pour passer des données du schéma `SAP_SILVER` au schéma `SAP_GOLD`. Ces transformations préparent les données pour les analyses et les rapports.

## Fonctionnalités

- **Transformation des données** : Conversion des données semi-traitées en tables prêtes pour l'analyse.
- **Application de la logique métier** : Enrichissement et agrégation des données.
- **Possibilité de Tests de qualité des données** : Validation de la cohérence et de l'intégrité des données.

## Prérequis

- **Compte Snowflake** avec les permissions nécessaires.
- **Installation de dbt** : Installez dbt et les adaptateurs requis :
  ```bash
  pip install dbt-core dbt-snowflake