# Rapport d'exploration des sources de données multimodales
## Projet CheckItAI : Pipeline d'acquisition texte + image pour la détection de fake news

---

## 1. Objectif

Identifier des sources exploitables pour constituer un corpus **multimodal** (texte + image) destiné à entraîner/évaluer un détecteur de fake news. Chaque publication du corpus doit, dans l'idéal, fournir :

- un **texte** (titre + corps ou légende),
- une **image** associée (illustration, capture d'écran, photo),
- une **date** de publication,
- une **provenance vérifiable** (URL, nom de domaine),
- un **label** vrai/faux exploitable, direct (dataset annoté) ou indirect (fiabilité de la source, verdict de fact-checking).

5 sources ont été évaluées (le brief en demandait au moins 3) afin de couvrir trois familles complémentaires : **API de presse** (texte + image, non labellisé), **réseaux sociaux** (texte + image, labellisation faible/communautaire), **datasets académiques pré-labellisés** (texte + image, labels forts), et **API de fact-checking** (texte, labels forts, peu/pas d'image).

---

## 2. Sources retenues

### 2.1 NewsAPI.org

| Critère | Détail |
|---|---|
| Modalités | Texte (titre, description, extrait) + `urlToImage` (URL d'image) |
| Format | JSON via REST |
| Langue | Multilingue (paramètre `language`), couverture FR correcte mais plus riche en EN |
| Qualité des labels | **Aucun label vrai/faux natif.** Champ `source.name` / `source.id` permet un classement indirect par réputation de la source |
| Méthode d'extraction | Appels REST authentifiés par clé API (`GET /v2/everything`, `GET /v2/top-headlines`), pagination par `page`/`pageSize`, téléchargement séparé des images via l'URL retournée |
| Contraintes | Plan gratuit : usage non commercial, développement local uniquement, ~100 requêtes/jour, délai de 24h sur les articles, pas de contenu intégral (extrait tronqué) |
| Points de vigilance | Le plan gratuit interdit explicitement l'usage en production  acceptable pour un POC/projet d'étude, à documenter dans les CGU du dataset final |

### 2.2 NewsData.io

| Critère | Détail |
|---|---|
| Modalités | Texte (titre, description, contenu) + `image_url` |
| Format | JSON via REST |
| Langue | ~89 langues déclarées, bon support du français, filtre `country`/`language` |
| Qualité des labels | Aucun label vrai/faux natif ; utile comme flux "à vérifier" plutôt que comme vérité terrain |
| Méthode d'extraction | REST + clé API, 200 crédits/jour en gratuit (10 articles/crédit), délai de 12h sur le contenu en gratuit |
| Points de vigilance | Redondant fonctionnellement avec NewsAPI  intérêt principal : meilleure couverture francophone et alternative si le quota NewsAPI est atteint |

### 2.3 Reddit (API officielle + PRAW)

| Critère | Détail |
|---|---|
| Modalités | Texte (titre, corps, commentaires) + image/lien média natif (posts `i.redd.it`, galeries) |
| Format | JSON via API REST officielle, wrapper Python `PRAW` |
| Langue | Majoritairement anglais ; sous-communautés (subreddits) francophones existent mais peu volumineuses |
| Qualité des labels | **Faible/indirecte** : pas de label fake/real officiel.  |
| Méthode d'extraction | OAuth2 obligatoire (*Responsible Builder Policy* : l'app doit être déclarée et approuvée par Reddit avant délivrance d'un token) ; quota 60 requêtes/min usage non commercial gratuit, usage commercial facturé |
| Points de vigilance | Ne pas confondre un post satirique/ironique (opinion, humour) avec de la désinformation |

### 2.4 Fakeddit (dataset pré-construit)

| Critère | Détail |
|---|---|
| Modalités | **Paires texte + image** natives (1M+ posts, 22 subreddits) |
| Format | TSV/CSV + dossier d'images, disponible sur Kaggle et GitHub (`entitize/Fakeddit`) |
| Langue | Anglais |
| Qualité des labels | **Bonne** : labellisation à 3 granularités (2 classes vrai/faux, 3 classes, 6 classes fines ex. satire, usurpation de source, contenu manipulé, fabriqué) obtenue par distant supervision depuis la réputation des subreddits d'origine |
| Méthode d'extraction | Téléchargement direct (pas de scraping à faire) : archive Kaggle/GitHub + script officiel de téléchargement des images par URL |
| Points de vigilance | Dataset figé (pas de flux temps réel) utile comme **base d'entraînement/référence**, pas comme source d'acquisition continue pour le pipeline "production" demandé par le lead technique |

### 2.5 FakeNewsNet (PolitiFact + GossipCop)

| Critère | Détail |
|---|---|
| Modalités | Texte (titre, corps d'article) + images de l'article + contexte social (métadonnées de partage) |
| Format | JSON/CSV via dépôt GitHub officiel (`KaiDMML/FakeNewsNet`), scripts de collecte fournis |
| Langue | Anglais |
| Qualité des labels | **Forte** : verdicts issus de fact-checkers professionnels. PolitiFact ("False"/"Pants on Fire" => fake, "True" => real) ; GossipCop (score de crédibilité => fake si faible) |
| Méthode d'extraction | Le dépôt fournit un script de collecte qui re-télécharge les articles (URLs) et un jeu d'IDs Twitter associés (soumis aux CGU Twitter/X, accès limité) |
| Points de vigilance | Certains liens d'articles sont morts (link rot) plusieurs années après collecte prévoir un taux de perte à l'extraction et un fallback (cache Wayback Machine) |

---

## 3. Tableau de synthèse comparatif

| Source | Texte | Image | Format | Langue | Labels | Extraction | Type d'accès |
|---|---|---|---|---|---|---|---|
| NewsAPI.org | Oui | Oui (URL) | JSON | Multi (FR correct) |  aucun | API REST | Officiel (clé API) |
| NewsData.io | Oui | Oui (URL) | JSON | Multi (89 langues) |  aucun | API REST | Officiel (clé API) |
| Reddit / PRAW | Oui | Oui (native) | JSON | EN majoritaire |  indirect (subreddit/flair) | API REST OAuth2 | Officiel (approbation requise) |
| Fakeddit | Oui | Oui (dataset) | TSV/CSV + images | EN |  bon (distant supervision, 3 granularités) | Téléchargement direct | Dataset académique ouvert |
| FakeNewsNet | Oui | Oui | JSON/CSV | EN |  fort (fact-checkers) | Scripts de collecte fournis | Dataset académique + re-collecte |


---

## 4. Champs indispensables retenus par publication

Pour que chaque entrée du corpus soit exploitable par le pipeline, les champs suivants sont considérés **indispensables** :

- `id` (identifiant unique interne, généré au pivot)
- `text` (titre + corps/extrait disponible)
- `image_url` / `image_path` (lien source + copie locale après téléchargement)
- `published_at` (date de publication)
- `source_url` (URL de la publication d'origine)
- `source_domain` (nom de domaine)
- `source_name` (média, subreddit, dataset d'origine)
- `label` (`fake` / `real` / `unlabeled`) + `label_confidence` ou `label_origin` (ex. `factcheck`, `distant_supervision`, `source_reputation`)
- `language`
- `collection_method` (`api`, `rss`, `dataset`) — traçabilité de la provenance


---

## 5. Format de sortie retenu

**JSON Lines (`.jsonl`)** comme format pivot interne, un objet JSON par ligne respectant le schéma ci-dessus (champs indispensables), plus un sous-dossier `images/` où chaque image est renommée avec l'`id` de l'entrée pour garantir l'association texte-image.

Justification :
- streaming-friendly (ajout incrémental sans recharger tout le fichier), adapté à un pipeline qui tourne sans intervention
- un objet = une publication => pas d'ambiguïté d'association champs/lignes contrairement à un CSV avec du texte libre (retours à la ligne, virgules)
- conversion triviale vers CSV ou Parquet en aval pour l'entraînement (`pandas.read_json(lines=True)` => `to_parquet`) si un format colonnaire plus compact est requis pour le stockage à grande échelle.

---

## 6. Fiabilité des labels & distinction opinion / désinformation

Point de vigilance explicite du brief : **une opinion controversée n'est pas une fake news**. Une opinion relève du jugement subjectif. la désinformation est une **affirmation factuelle objectivement fausse**, diffusée pour tromper.

Conséquences pour la sélection de sources :
- Les sources à label **fort** (Fakeddit, FakeNewsNet) labellisent des **affirmations factuelles vérifiées**, pas des opinions, elles sont donc directement utilisables comme vérité terrain.
- Les sources à label **indirect** (Reddit par subreddit) mesurent une **probabilité de fiabilité de la source**, pas la véracité de chaque publication individuellement. Un post satirique ou un édito d'opinion dans un média fiable ne doivent pas être automatiquement étiquetés "fake" ou "real" sans relecture, ce sont des cas limites à isoler dans une classe `ambiguous`/`opinion` plutôt que de les forcer dans le binaire vrai/faux.

---

## 7. Droits d'usage et conformité

| Source | Statut d'usage |
|---|---|
| NewsAPI.org / NewsData.io | Gratuit = non-commercial / développement uniquement ; usage projet d'étude conforme |
| Reddit | Gratuit tant que non-commercial, **approbation d'app obligatoire** depuis la Responsible Builder Policy à demander en amont, prévoir un délai |
| Fakeddit / FakeNewsNet | Datasets académiques diffusés pour la recherche, citer les papiers d'origine, usage non commercial |


