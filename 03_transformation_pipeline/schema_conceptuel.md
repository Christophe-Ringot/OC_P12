# Schéma conceptuel des données

## Diagramme entité-relation

```mermaid
erDiagram
    SOURCE ||--o{ PUBLICATION : "publie"

    SOURCE {
        texte nom
        texte domaine
        categorie type_source
    }

    PUBLICATION {
        identifiant id
        texte contenu
        nombre longueur_texte
        nombre nombre_mots
        date date_publication
        categorie langue
        categorie label
        categorie origine_label
        url image
        chemin image_locale
        booleen a_une_image
        booleen image_valide
        booleen texte_image_associes
        categorie methode_collecte
        date date_traitement
    }
```

Cardinalité : une **Source** publie zéro, une ou plusieurs **Publications** ; chaque **Publication** provient d'exactement une **Source**.
