

# Projet fil rouge – **BikeOps : prédire la disponibilité des vélos en libre-service**

---

## Objectif global

Une collectivité souhaite disposer d’une plateforme data complète permettant de :

1. Collecter et nettoyer des données brutes sur la disponibilité des vélos et la météo.
2. Mettre en place une architecture **bronze → silver → gold** afin d’assurer la qualité et la performance.
3. Réaliser des analyses avec Spark SQL et DataFrames.
4. Développer un modèle de **machine learning** pour prédire la disponibilité des vélos.
5. Déployer l’ensemble de la chaîne sur le **cloud GCP**, avec intégration CI/CD, conteneurs et monitoring.

Les données sont volontairement bruitées (formats incohérents, valeurs manquantes, dépassements, nulls) afin de travailler sur leur correction et d’assurer la fiabilité des pipelines.

---

## Données fournies (bronze)

1. **stations.csv** (propre)

   * `station_id`, `station_name`, `lat`, `lon`, `capacity`
   * Environ 25 stations

2. **availability\_raw\.csv** (bruité, pas 5 min)

   * `station_id`, `timestamp`, `bikes_available`, `slots_free`
   * Défauts : multi-formats de date, valeurs `null/n/a/?/NaN`, valeurs négatives, dépassements de capacité, séparateurs décimaux incorrects.

3. **weather\_raw\.csv** (bruité, pas horaire)

   * `timestamp`, `temperature_c`, `rain_mm`, `weather_condition`
   * Défauts : dates incohérentes, valeurs nulles, décimales au mauvais format, météo manquante.

---

# Étapes du projet

---

## Étape 0 – Contrat de données et cadrage

**Contexte** : comprendre et formaliser les règles de qualité avant toute ingestion.

**Travail attendu** :

* Identifier les incohérences et pièges dans `availability_raw` et `weather_raw`.
* Définir un contrat de données (types attendus, contraintes, clés, règles de correction).
* Proposer un dictionnaire de données clair et structuré.

**Livrables** :

* Dictionnaire de données.
* Contrat de données.
* Exemple de 10 lignes corrigées manuellement.

---

## Étape 1 – Ingestion batch et Silver (Spark enrichi)

**Contexte** : transformer les données brutes en tables Silver exploitables.

**Travail attendu** :

* Lire les CSV bruités avec Spark (séparateur `;`).
* Tester les modes d’ingestion Spark : `PERMISSIVE`, `DROPMALFORMED`, `DROPMALFORMED`.
* Développer des UDF Spark pour :

  * uniformiser les timestamps multi-formats,
  * corriger les décimales.
  * Corriger les valeurs aberrantes (`0 ≤ bikes_available ≤ capacity`).
* Dédupliquer sur `(station_id, timestamp)`.
* Sauvegarder en Parquet partitionné par `date`.

**Livrables** :

* Scripts `ingestion_availability.py`, `ingestion_weather.py`.
* Tables Silver (`availability_silver`, `weather_silver`, `stations_silver`).
* Rapport qualité (lignes brutes, corrigées, supprimées).

---

## Étape 2 – Zone Gold et premiers indicateurs

**Contexte** : fournir un reporting quotidien fiable.

**Travail attendu** :

* Joindre `availability_silver` avec `stations_silver` pour enrichir avec la capacité.
* Joindre avec `weather_silver` (arrondi à l’heure) pour ajouter la météo.
* Construire une table Gold quotidienne contenant :

  * vélos moyens disponibles,
  * taux d’occupation,
  * météo dominante,
  * top 5 stations les plus saturées.
* Concevoir un schéma en étoile (faits + dimensions).
* Interroger les données avec Spark SQL.

**Livrables** :

* Script `gold_daily.py`.
* Table `availability_daily_gold`.
* Notebook Spark SQL avec au moins 3 requêtes.

---

## Étape 3 – Qualité et tests

**Contexte** : garantir la robustesse et la fiabilité du pipeline.

**Travail attendu** :

* Ajouter des tests unitaires PyTest pour les fonctions de parsing.
* Écrire des règles de contrôle qualité Spark :

  * absence de valeurs négatives,
  * cohérence vélos disponibles + slots libres = capacité,
  * détection de stations figées.
* Générer un rapport qualité distribué (par station et par jour).

**Livrables** :

* Script `quality_check.py`.
* Table `quality_report`.
* Tests unitaires dans le dossier `tests/`.

---

## Étape 4 – Premier modèle de Machine Learning

**Contexte** : prévoir la disponibilité à court terme.

**Travail attendu** :

* Construire un dataset de features (fenêtres temporelles, météo, taux d’occupation retardé).
* Implémenter une baseline simple (persistance, moyenne mobile).
* Implémenter un modèle TensorFlow/Keras (régression simple).
* Comparer baseline et modèle selon RMSE/MAE.

**Livrables** :

* Notebook `ml_baseline_vs_tf.ipynb`.
* Artefacts de modèles sauvegardés.
* Rapport comparatif baseline vs modèle.

---

## Étape 5 – Batch scoring et restitution

**Contexte** : générer et exploiter des prédictions quotidiennes.

**Travail attendu** :

* Développer un job Spark de scoring J+1.
* Stocker les résultats dans `predictions_gold`.
* Créer un tableau de bord simple (matplotlib ou Streamlit).

**Livrables** :

* Script `batch_scoring.py`.
* Table `predictions_gold`.
* Dashboard simple.

---

## Étape 6 – Conteneurisation et CI/CD

**Contexte** : industrialiser les développements.

**Travail attendu** :

* Écrire des Dockerfiles pour ETL, training et scoring.
* Développer un pipeline CI/CD : lint, tests, build, scan basique, publication d’images.

**Livrables** :

* Dockerfiles et `docker-compose.yml`.
* Pipeline CI/CD (GitHub Actions ou GitLab CI).

---

## Étape 7 – Streaming (Spark Structured Streaming)

**Contexte** : surveiller en temps quasi réel la disponibilité.

**Travail attendu** :

* Créer un simulateur d’événements (Python → Kafka).
* Développer un job Spark Streaming alimentant `availability_realtime_silver`.
* Détecter les stations critiques (<20 % de vélos restants >30 minutes).

**Livrables** :

* Script `event_producer.py`.
* Job `streaming_ingestion.py`.
* Table `availability_realtime_silver`.

---

## Étape 8 – Migration vers le Cloud (GCP)

**Contexte** : passage à l’échelle.

**Travail attendu** :

* Stocker les données sur Google Cloud Storage (bronze/silver/gold).
* Utiliser Spark sur Dataproc.
* Exploiter BigQuery pour le Gold et les prédictions.
* Mettre en place Pub/Sub pour le streaming.

**Livrables** :

* Scripts ingestion GCS + BigQuery.
* Tables BigQuery.
* Document d’architecture GCP.

---

## Étape 9 – Orchestration et observabilité

**Contexte** : automatiser et superviser le pipeline.

**Travail attendu** :

* Développer un DAG Airflow (ETL → ML → scoring → publication).
* Mettre en place des métriques Prometheus/Grafana.
* Configurer des alertes simples (échec job, latence élevée).

**Livrables** :

* Script `bikeops_dag.py`.
* Dashboard Grafana.

---

## Synthèse des livrables

* **Étapes 0–3 (Spark)** : ingestion, nettoyage, zone Gold, qualité et tests.
* **Étapes 4–5 (ML)** : baseline, modèle TensorFlow, scoring batch et restitution.
* **Étapes 6–7 (DevOps)** : conteneurisation, CI/CD, streaming Spark.
* **Étapes 8–9 (Cloud)** : migration GCP, orchestration et observabilité.
