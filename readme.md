Voici un modèle de `README.md` que tu peux mettre à la racine du projet et adapter légèrement si besoin.

***

# Spark_Proj_AF – BAL French Addresses Incremental Storage

Ce projet implémente une solution automatisée de traitement Big Data avec Apache Spark pour stocker et historiser les adresses françaises (« BAL ») de façon incrémentale pendant au moins 30 ans, en optimisant le coût de stockage.

L’objectif est de :
- Intégrer chaque jour un dump CSV BAL.
- Ne stocker que les **différences journalières** (insert/update/delete) en Parquet.
- Maintenir un snapshot **`bal_latest`** toujours à jour.
- Pouvoir reconstruire un dump complet à une date donnée.
- Comparer deux dumps complets (diff).

***

## 1. Architecture générale

Données stockées localement à la racine du projet :

- `bal.db/bal_latest/`  
  Snapshot **complet courant** (Parquet).

- `bal.db/bal_diff/day=YYYY-MM-DD/`  
  Diffs journaliers (Parquet), partitionnés par date, avec une colonne `op` ∈ {`INSERT`, `UPDATE`, `DELETE`}.

Le cœur de la logique est dans les classes Java :

- `fr.esilv.spark.DailyJob` / `DailyMain` : intégration quotidienne.
- `fr.esilv.spark.ReportJob` / `ReportMain` : génération de rapports.
- `fr.esilv.spark.RecomputeJob` / `RecomputeMain` : reconstruction d’un dump complet à une date donnée.
- `fr.esilv.spark.DiffJob` / `DiffMain` : calcul de diff entre deux dumps Parquet.

***

## 2. Prérequis

- Java 17 installé.
- Apache Spark 3.5.x installé et disponible dans le `PATH` (`spark-submit` accessible).
- Maven installé (`mvn`).

***

## 3. Build du projet

À la racine du projet :

```bash
mvn clean package
```

Cela produit notamment :

- `target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar` (JAR exécutable avec toutes les dépendances).

***

## 4. Scripts disponibles

Quatre scripts Bash sont fournis à la racine du projet.

### 4.1 Intégration quotidienne

```bash
./run_daily_file_integration.sh <date> <csv_file>
```

Exemple :

```bash
./run_daily_file_integration.sh 2025-01-01 /path/to/dump-2025-01-01.csv
```

Fonctionnement :

- Lit le CSV du jour (en inférant le schéma, adaptation possible pour des colonnes comme `id` → `uid_adresse`).
- Compare avec `bal.db/bal_latest` (si présent).
- Calcule `INSERT` / `UPDATE` / `DELETE` pour la clé métier `uid_adresse`.
- Écrit le diff du jour dans `bal.db/bal_diff/day=<date>/`.
- Met à jour le snapshot complet `bal.db/bal_latest/`.

### 4.2 Rapport sur le dernier snapshot

```bash
./run_report.sh
```

- Lit `bal.db/bal_latest/`.
- Affiche des statistiques globales (nombre total d’adresses, schéma, échantillon…).

### 4.3 Reconstruction d’un dump à une date donnée

```bash
./recompute_and_extract_dump_at_date.sh <date> <output_dir>
```

Exemple :

```bash
./recompute_and_extract_dump_at_date.sh 2025-01-24 /tmp/recap_dumpA
```

- Rejoue l’historique des diffs jusqu’à `<date>`.
- Écrit un dump complet en Parquet dans `<output_dir>`.

### 4.4 Diff entre deux dumps Parquet

```bash
./compute_diff_between_files.sh <parquet_dir_1> <parquet_dir_2>
```

Exemple :

```bash
./compute_diff_between_files.sh /tmp/recap_dumpA /tmp/recap_dumpB
```

- Lit les deux jeux de données Parquet.
- Compare sur la clé `uid_adresse`.
- Comptabilise les lignes **ajoutées**, **supprimées** et **modifiées** entre 1 et 2.

***

## 5. Test d’intégration complet

Un script d’intégration est fourni :

```bash
./run_test_scenario.sh
```

Ce script :

1. Génère 50 jours de dumps CSV mock dans `/tmp/spark_project_test/inputs`.
2. Appelle `run_daily_file_integration.sh` pour chaque jour, enchaînant les mises à jour de `bal_diff` et `bal_latest`.
3. Lance périodiquement `run_report.sh`.
4. Reconstruit deux dumps complets à deux dates différentes avec `recompute_and_extract_dump_at_date.sh`.
5. Compare ces deux dumps avec `compute_diff_between_files.sh`.

À la fin, la structure attendue est :

- `bal.db/bal_latest/` : dernier snapshot en Parquet.
- `bal.db/bal_diff/day=YYYY-MM-DD/` : un fichier Parquet par jour (50 partitions dans le scénario complet).

***

## 6. Nettoyage

Pour effacer les données générées (utile avant un nouveau test) :

```bash
rm -rf bal.db /tmp/spark_project_test
```

Les fichiers sources (`src/`), scripts `.sh`, `pom.xml` et JAR dans `target/` restent intacts.

***

Tu peux coller ce contenu dans un `README.md` et l’ajuster au besoin (par exemple en ajoutant 1–2 screenshots de `Pics/` dans le rapport plutôt que dans le README).