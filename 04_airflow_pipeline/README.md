# Étape 4 — Pipeline ETL Apache Airflow

DAG `checkitai_etl` : extraction multimodale (étape 2) → nettoyage (étape 3) → chargement dans PostgreSQL. Les fonctions des étapes 2 et 3 sont importées directement dans le DAG (pas dupliquées), conformément à la recommandation du brief.

```
extract_rss ─┐
extract_newsapi ─┤
extract_newsdata ─┼─► transform ─► load_to_postgres
extract_fakenewsnet ─┘
```

## 1. Pourquoi PostgreSQL (SQL) plutôt qu'une base NoSQL

Le modèle conceptuel finalisé à l'étape 3 ([schema_conceptuel.md](../03_transformation_pipeline/schema_conceptuel.md)) définit deux entités à schéma fixe et typé (`Source`, `Publication`) reliées par une relation 1-N claire — exactement le cas d'usage pour lequel le relationnel excelle : intégrité référentielle (`FOREIGN KEY`), contraintes de validité (`CHECK` sur `label`/`source_type`), et requêtes agrégées immédiates pour le monitoring (`GROUP BY label`, `GROUP BY source_name`). Une base NoSQL orientée document aurait eu du sens si le schéma restait irrégulier/imbriqué (ex. JSON brut de l'étape 1 avant nettoyage, avec champs optionnels variables selon la source) — ce n'est plus le cas après l'étape 3, qui produit précisément un schéma stable. PostgreSQL est donc le choix pertinent ici, pas un choix par défaut.

## 2. Prérequis

- Docker Desktop (déjà présent sur ce poste).
- Aucune installation Python d'Airflow nécessaire : tout tourne en conteneurs.

## 3. Configuration (secrets)

```bash
cd 04_airflow_pipeline
cp .env.example .env
```

Générer les deux clés et les coller dans `.env` :

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # -> AIRFLOW_FERNET_KEY
python -c "import secrets; print(secrets.token_hex(16))"                                     # -> AIRFLOW_SECRET_KEY
```

Renseigner aussi dans `.env` : `AIRFLOW_ADMIN_PASSWORD` (connexion à l'UI), `POSTGRES_AIRFLOW_PASSWORD`, `POSTGRES_CHECKITAI_ADMIN_PASSWORD`, `CHECKITAI_ETL_PASSWORD` (mots de passe forts, différents). `.env` est exclu de git.

## 4. Démarrage

```bash
docker compose up -d --build
```

Premier démarrage : `airflow-init` migre la base de métadonnées et crée l'utilisateur admin, puis s'arrête (normal). `airflow-webserver` et `airflow-scheduler` démarrent ensuite. UI disponible sur **http://localhost:8080** (identifiants : `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` du `.env`) — c'est la méthode d'authentification de l'interface (Flask-AppBuilder, `basic_auth`).

## 5. Connecter le DAG à la base cible (Connection Airflow chiffrée)

Le DAG ne contient aucun mot de passe en dur : il récupère la Connection Airflow `checkitai_postgres` au moment de l'exécution (`BaseHook.get_connection`), stockée chiffrée (Fernet) dans la base de métadonnées Airflow.

```bash
docker compose exec airflow-webserver airflow connections add checkitai_postgres \
  --conn-type postgres \
  --conn-host postgres-checkitai \
  --conn-port 5432 \
  --conn-schema checkitai \
  --conn-login checkitai_etl \
  --conn-password "<valeur de CHECKITAI_ETL_PASSWORD dans .env>"
```

Important : `checkitai_etl` est le **rôle applicatif à privilèges limités** créé par `sql/init_checkitai_db.sh` (`SELECT`/`INSERT`/`UPDATE` sur `sources`/`publications` uniquement, pas de DDL, pas de `DELETE`) — jamais le compte superuser Postgres (`POSTGRES_CHECKITAI_ADMIN_PASSWORD`), qui ne sert qu'à l'initialisation du conteneur.

## 6. (Optionnel) Clés API pour NewsAPI / NewsData.io

Sans ces Variables, les tâches `extract_newsapi`/`extract_newsdata` se terminent avec succès mais ne rapportent aucune publication (comportement identique au `main.py` de l'étape 2 : source isolée, pas d'échec du DAG).

```bash
docker compose exec airflow-webserver airflow variables set NEWSAPI_KEY "<clé>"
docker compose exec airflow-webserver airflow variables set NEWSDATA_KEY "<clé>"
docker compose exec airflow-webserver airflow variables set checkitai_query "climat"
```

Ces Variables sont elles aussi chiffrées au repos (même clé Fernet).

## 7. Exécuter le DAG

Le DAG est créé en pause (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`). Depuis l'UI : activer le toggle `checkitai_etl` puis bouton ▶ "Trigger DAG". En CLI :

```bash
docker compose exec airflow-webserver airflow dags unpause checkitai_etl
docker compose exec airflow-webserver airflow dags trigger checkitai_etl
```

## 8. Vérifier le résultat

```bash
docker compose exec postgres-checkitai psql -U checkitai_etl -d checkitai \
  -c "SELECT label, count(*) FROM publications GROUP BY label;"

docker compose exec postgres-checkitai psql -U checkitai_etl -d checkitai \
  -c "SELECT source_name, count(*) FROM publications GROUP BY source_name ORDER BY 2 DESC;"
```

Preuves d'exécution à capturer pour le livrable (dossier `screenshots/`) :
1. Vue Graph du DAG dans l'UI (`checkitai_etl`, toutes les tâches en vert).
2. Logs d'une tâche d'extraction (ex. `extract_fakenewsnet`) montrant le nombre de publications récupérées.
3. Résultat des deux requêtes SQL ci-dessus.

## 9. Sécurité — récapitulatif

| Exigence du brief | Mise en œuvre |
|---|---|
| Méthode d'authentification | UI Airflow : Flask-AppBuilder `basic_auth`, compte admin dédié (pas de compte par défaut). Base cible : authentification Postgres par mot de passe (rôle `checkitai_etl`). |
| Accès limité aux rôles autorisés | `checkitai_etl` n'a que `SELECT`/`INSERT`/`UPDATE` sur `sources`/`publications` — pas de DDL, pas de `DELETE`, pas d'accès à la base `airflow` (métadonnées internes, sur un conteneur Postgres séparé). Voir `sql/init_checkitai_db.sh`. |
| Chiffrement des données sensibles | Les identifiants (mot de passe Postgres, clés API) ne transitent jamais en clair dans le code du DAG : Connections/Variables Airflow, chiffrées au repos avec `AIRFLOW__CORE__FERNET_KEY`. Le contenu du dataset lui-même (titres/images de presse) est public, donc non concerné — mais l'extension `pgcrypto` est activée sur la base cible, prête à chiffrer une colonne si un champ sensible (ex. donnée utilisateur) était ajouté plus tard. |

## 10. Limites connues (démo locale)

- `EXTRACT_LIMIT=10` et `enrich_images=False` sur FakeNewsNet : volontairement bas pour des tâches courtes (point de vigilance du brief) — remonter ces valeurs dans `dags/checkitai_etl_dag.py` pour un run à plus grande échelle, sur le modèle des runs manuels de l'étape 2.
- Connexion Postgres cible sans TLS (trafic local uniquement, ne quitte pas la machine) : en production, ajouter `sslmode=require` à la Connection.

## 11. Vérification déjà effectuée (sans Docker)

Docker Desktop n'avait pas de moteur WSL2 fonctionnel sur le poste de développement au moment de finaliser cette étape. Plutôt que de livrer un DAG non testé, il a été validé avec un **vrai Airflow 2.10.4** installé en local via pip (`venv_local_verify/`, non versionné), métadonnées sur SQLite, `AIRFLOW__CORE__EXECUTOR=SequentialExecutor` :

```bash
airflow dags list-import-errors     # aucune erreur de parsing
airflow dags test checkitai_etl 2026-08-23
```

Résultat réel obtenu : `extract_rss` (10/10), `extract_newsapi` (10/10), `extract_newsdata` (10/10), `extract_fakenewsnet` (12 publications, 4 catégories) et `transform` (42 publications en entrée, 42 en sortie, 0 rejet, 0 doublon) **tous en SUCCESS**. `load_to_postgres` échoue avec `AirflowNotFoundException: The conn_id 'checkitai_postgres' isn't defined` — attendu, aucun Postgres n'était disponible pour créer cette Connection. La logique de cette tâche (construction des lignes, parsing des dates hétérogènes via `_to_timestamp`) a été vérifiée séparément hors-ligne sur un vrai batch de publications transformées : tous les champs se typent correctement (dates -> `datetime` timezone-aware, booléens, `None` gérés).

Deux bugs réels ont été trouvés et corrigés pendant cette vérification (des messages de log utilisant `{variable}` sans préfixe `f`, donc jamais interpolés, dans `newsapi_extractor.py` et `newsdata_extractor.py` — cosmétique, sans impact sur les données, mais corrigé).

**Reste à faire sur un poste avec Docker fonctionnel** (`git pull` puis suivre les sections 2 à 8) : `docker compose up`, créer la Connection `checkitai_postgres`, déclencher le DAG, et capturer les captures d'écran de l'UI + le résultat des requêtes SQL comme preuves d'exécution finales du livrable.
