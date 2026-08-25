#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Disponible pour chiffrer une future colonne sensible (aucun champ du dataset actuel
    -- ne l'exige : ce sont des titres/images de presse publics, pas des données
    -- personnelles ; voir README.md, section Sécurité).
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

    CREATE TABLE IF NOT EXISTS sources (
        source_name   TEXT PRIMARY KEY,
        source_domain TEXT NOT NULL,
        source_type   TEXT NOT NULL CHECK (source_type IN ('editorial_api', 'aggregator', 'academic_dataset', 'unknown'))
    );

    CREATE TABLE IF NOT EXISTS publications (
        id                 TEXT PRIMARY KEY,
        text_clean         TEXT NOT NULL,
        text_length        INTEGER NOT NULL,
        word_count         INTEGER NOT NULL,
        published_at       TIMESTAMPTZ,
        source_name        TEXT NOT NULL REFERENCES sources(source_name),
        language           TEXT NOT NULL,
        label              TEXT NOT NULL CHECK (label IN ('fake', 'real', 'unlabeled')),
        label_origin       TEXT NOT NULL,
        image_url          TEXT,
        image_path         TEXT,
        has_image          BOOLEAN NOT NULL,
        image_valid        BOOLEAN NOT NULL,
        text_image_aligned BOOLEAN NOT NULL,
        collection_method  TEXT NOT NULL,
        processed_at       TIMESTAMPTZ NOT NULL,
        loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_publications_label       ON publications(label);
    CREATE INDEX IF NOT EXISTS idx_publications_source_name ON publications(source_name);

    -- Rôle dédié au DAG : ni superuser, ni droits DDL, ni accès à la base "airflow"
    -- (métadonnées internes d'Airflow, sur un conteneur Postgres séparé de toute façon).
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'checkitai_etl') THEN
            CREATE ROLE checkitai_etl LOGIN PASSWORD '${CHECKITAI_ETL_PASSWORD}';
        END IF;
    END
    \$\$;

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO checkitai_etl;
    GRANT USAGE ON SCHEMA public TO checkitai_etl;
    GRANT SELECT, INSERT, UPDATE ON sources, publications TO checkitai_etl;
    -- Explicitement PAS de DELETE (l'ETL est additif/idempotent via upsert, jamais destructif)
    -- et PAS de droits sur pg_roles/pg_authid (impossible de lister ou modifier d'autres rôles).
EOSQL
