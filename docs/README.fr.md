🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [English](../README.md) | [Español](README.es.md) | [Deutsch](README.de.md) | Français | [日本語](README.ja.md)

<p align="center">
  <img src="logo.png" alt="AIVectorMemory Logo" width="200">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>Donnez une mémoire à votre assistant IA — Serveur MCP de mémoire persistante inter-sessions</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>
---

> **Vous utilisez encore CLAUDE.md / MEMORY.md comme mémoire ?** Cette approche mémoire par fichier Markdown a des défauts fatals : le fichier grossit sans cesse, injectant tout à chaque session et consumant une quantité massive de tokens ; le contenu ne supporte que la recherche par mots-clés — chercher « timeout base de données » ne trouve pas « piège du pool de connexions MySQL » ; partager un fichier entre projets cause une contamination croisée ; il n'y a pas de suivi des tâches, la progression du développement repose uniquement sur votre mémoire ; sans parler de la troncature à 200 lignes, de la maintenance manuelle et de l'impossibilité de dédupliquer ou fusionner.
>
> **AIVectorMemory est une approche fondamentalement différente.** Stockage en base vectorielle locale avec recherche sémantique pour un rappel précis (trouve les correspondances même avec des termes différents), récupération à la demande qui ne charge que les mémoires pertinentes (consommation de tokens en baisse de 50%+), isolation multi-projets automatique sans interférence, et suivi des problèmes + gestion des tâches intégrés permettant à l'IA d'automatiser entièrement votre workflow de développement. Toutes les données sont stockées de façon permanente sur votre machine — zéro dépendance cloud, rien ne se perd en changeant de session ou d'IDE.

## ✨ Fonctionnalités Principales

| Fonctionnalité | Description |
|----------------|-------------|
| 🧠 **Mémoire Inter-Sessions** | Votre IA se souvient enfin de votre projet — erreurs rencontrées, décisions prises, conventions établies, tout persiste entre les sessions |
| 🔍 **Recherche Sémantique** | Pas besoin de se rappeler les mots exacts — chercher « timeout base de données » trouve « erreur pool de connexions MySQL » |
| 💰 **Économie 50%+ Tokens** | Fini le copier-coller du contexte projet à chaque conversation. Récupération sémantique à la demande, adieu l'injection massive |
| 🔗 **Dev Piloté par Tâches** | Suivi des problèmes → découpage en tâches → synchronisation des statuts → archivage lié. L'IA gère tout le workflow de développement |
| 📊 **Tableau de Bord Web** | Gestion visuelle de toutes les mémoires et tâches, réseau vectoriel 3D pour voir les connexions de connaissances d'un coup d'œil |
| 🏠 **Entièrement Local** | Zéro dépendance cloud. Inférence locale ONNX, pas de clé API, les données ne quittent jamais votre machine |
| 🔌 **Tous les IDEs** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — installation en un clic, prêt à l'emploi |
| 📁 **Isolation Multi-Projets** | Une seule BD pour tous les projets, isolation automatique sans interférence, changement de projet transparent |
| 🔄 **Déduplication Intelligente** | Similarité > 0.95 fusionne automatiquement, la base de mémoires reste propre — ne devient jamais désordonnée |

<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  共同参与项目开发加QQ群或微信交流
</p>

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────┐
│                   AI IDE                         │
│  OpenCode / Claude Code / Cursor / Kiro / ...   │
└──────────────────────┬──────────────────────────┘
                       │ MCP Protocol (stdio)
┌──────────────────────▼──────────────────────────┐
│              AIVectorMemory Server               │
│                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ remember │ │  recall   │ │   auto_save      │ │
│  │ forget   │ │  task     │ │   status/track   │ │
│  └────┬─────┘ └────┬─────┘ └───────┬──────────┘ │
│       │            │               │             │
│  ┌────▼────────────▼───────────────▼──────────┐  │
│  │         Embedding Engine (ONNX)            │  │
│  │      intfloat/multilingual-e5-small        │  │
│  └────────────────────┬───────────────────────┘  │
│                       │                          │
│  ┌────────────────────▼───────────────────────┐  │
│  │     SQLite + sqlite-vec (Index Vectoriel)  │  │
│  │     ~/.aivectormemory/memory.db            │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 🚀 Démarrage Rapide

### Option 1 : Installation pip (Recommandé)

```bash
# Installer
pip install aivectormemory

# Mettre à jour vers la dernière version
pip install --upgrade aivectormemory

# Aller dans votre répertoire de projet, configuration IDE en un clic
cd /path/to/your/project
run install
```

`run install` vous guide interactivement pour choisir votre IDE, génère automatiquement la configuration MCP, les règles Steering et les Hooks — aucune configuration manuelle nécessaire.

> **Utilisateurs macOS attention** :
> - En cas d'erreur `externally-managed-environment`, ajoutez `--break-system-packages`
> - En cas d'erreur `enable_load_extension`, votre Python ne supporte pas le chargement d'extensions SQLite (le Python intégré de macOS et les installateurs python.org ne le supportent pas). Utilisez Homebrew Python :
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### Option 2 : uvx (sans installation)

Pas besoin de `pip install`, exécutez directement :

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> [uv](https://docs.astral.sh/uv/getting-started/installation/) doit être installé. `uvx` télécharge et exécute automatiquement le paquet.

### Option 3 : Configuration manuelle

```json
{
  "mcpServers": {
    "aivectormemory": {
      "command": "run",
      "args": ["--project-dir", "/path/to/your/project"]
    }
  }
}
```

<details>
<summary>📍 Emplacements des fichiers de configuration par IDE</summary>

| IDE | Chemin de configuration |
|-----|------------------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8 Outils MCP

### `remember` — Stocker une mémoire

```
content (string, requis)   Contenu au format Markdown
tags    (string[], requis)  Étiquettes, ex. ["erreur", "python"]
scope   (string)            "project" (par défaut) / "user" (inter-projets)
```

Similarité > 0.95 met à jour automatiquement la mémoire existante, sans doublons.

### `recall` — Recherche sémantique

```
query   (string)     Mots-clés de recherche sémantique
tags    (string[])   Filtre exact par étiquettes
scope   (string)     "project" / "user" / "all"
top_k   (integer)    Nombre de résultats, par défaut 5
```

Correspondance par similarité vectorielle — trouve des mémoires liées même avec des mots différents.

### `forget` — Supprimer des mémoires

```
memory_id  (string)     ID unique
memory_ids (string[])   IDs en lot
```

### `status` — État de session

```
state (object, optionnel)   Omettre pour lire, passer pour mettre à jour
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

Maintient la progression du travail entre les sessions, restaure automatiquement le contexte.

### `track` — Suivi des problèmes

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   Titre du problème
issue_id (integer)  ID du problème
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   Contenu d'investigation
```

### `task` — Gestion des tâches

```
action     (string, requis)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)          Identifiant de fonctionnalité associée (requis pour list)
tasks      (array)           Liste de tâches (batch_create, sous-tâches supportées)
task_id    (integer)         ID de tâche (update)
status     (string)          "pending" / "in_progress" / "completed" / "skipped"
```

Lié aux documents spec via feature_id. Update synchronise automatiquement les checkboxes tasks.md et le statut des problèmes associés.

### `readme` — Génération de README

```
action   (string)    "generate" (par défaut) / "diff" (comparer les différences)
lang     (string)    Langue : en / zh-TW / ja / de / fr / es
sections (string[])  Sections spécifiques : header / tools / deps
```

Génère automatiquement le contenu README depuis TOOL_DEFINITIONS / pyproject.toml, support multilingue.

### `auto_save` — Sauvegarde automatique des préférences

```
preferences  (string[])  Préférences techniques exprimées par l'utilisateur (scope=user fixe, inter-projets)
extra_tags   (string[])  Étiquettes supplémentaires
```

Extrait et stocke automatiquement les préférences utilisateur à la fin de chaque conversation, déduplication intelligente.

## 📊 Tableau de Bord Web

```bash
run web --port 9080
run web --port 9080 --quiet          # Supprimer les logs de requêtes
run web --port 9080 --quiet --daemon  # Exécuter en arrière-plan (macOS/Linux)
```

Visitez `http://localhost:9080` dans votre navigateur. Nom d'utilisateur par défaut `admin`, mot de passe `admin123` (modifiable dans les paramètres après la première connexion).

- Basculement entre projets, parcourir/rechercher/modifier/supprimer/exporter/importer les mémoires
- Recherche sémantique (correspondance par similarité vectorielle)
- Suppression des données de projet en un clic
- État de session, suivi des problèmes
- Gestion des étiquettes (renommer, fusionner, suppression par lots)
- Protection par authentification Token
- Visualisation 3D du réseau vectoriel de mémoires
- 🌐 Support multilingue (简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語)

<p align="center">
  <img src="003.png" alt="Connexion" width="100%">
  <br>
  <em>Connexion</em>
</p>

<p align="center">
  <img src="dashboard-projects.png" alt="Sélection de Projet" width="100%">
  <br>
  <em>Sélection de Projet</em>
</p>

<p align="center">
  <img src="dashboard-overview.png" alt="Aperçu & Visualisation du Réseau Vectoriel" width="100%">
  <br>
  <em>Aperçu & Visualisation du Réseau Vectoriel</em>
</p>

<p align="center">
  <img src="20260306234753_6_1635.jpg" alt="Groupe WeChat" width="280">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="8_1635.jpg" alt="Groupe QQ : 1085682431" width="280">
  <br>
  <em>Rejoindre le groupe WeChat &nbsp;|&nbsp; Rejoindre le groupe QQ</em>
</p>

## ⚡ Combinaison avec les Règles Steering

AIVectorMemory est la couche de stockage. Utilisez les règles Steering pour indiquer à l'IA **quand et comment** appeler ces outils.

L'exécution de `run install` génère automatiquement les règles Steering et la configuration des Hooks — aucune configuration manuelle nécessaire.

| IDE | Emplacement Steering | Hooks |
|-----|---------------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md` (ajouté) | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md` (ajouté) | `.claude/settings.json` |
| Trae | `.trae/rules/aivectormemory.md` | — |
| OpenCode | `AGENTS.md` (ajouté) | `.opencode/plugins/*.js` |

<details>
<summary>📋 Exemple de Règles Steering (généré automatiquement)</summary>

```markdown
# AIVectorMemory - Règles de Workflow

## 1. Démarrage de Nouvelle Session (exécuter dans l'ordre)

1. `recall` (tags: ["connaissance-projet"], scope: "project", top_k: 100) charger les connaissances du projet
2. `recall` (tags: ["preference"], scope: "user", top_k: 20) charger les préférences utilisateur
3. `status` (sans paramètre state) lire l'état de la session
4. Bloqué → signaler et attendre ; Non bloqué → entrer dans le flux de traitement

## 2. Flux de Traitement des Messages

- Étape A : `status` lire l'état, attendre si bloqué
- Étape B : Classifier le type de message (discussion/correction/préférence/problème de code)
- Étape C : `track create` enregistrer le problème
- Étape D : Investiguer (`recall` chercher les erreurs + examiner le code + trouver la cause racine)
- Étape E : Présenter le plan à l'utilisateur, bloquer en attente de confirmation
- Étape F : Modifier le code (`recall` vérifier les erreurs avant modification)
- Étape G : Exécuter les tests pour vérifier
- Étape H : Bloquer en attente de vérification utilisateur
- Étape I : Utilisateur confirme → `track archive` + débloquer

## 3. Règles de Blocage

Doit `status({ is_blocked: true })` lors de propositions de plans ou d'attente de vérification.
Débloquer uniquement après confirmation explicite de l'utilisateur. Jamais d'auto-déblocage.

## 4-9. Suivi des Problèmes / Vérification du Code / Gestion Spec/Tâches / Qualité Mémoire / Référence Outils / Standards de Développement

(Règles complètes générées automatiquement par `run install`)
```

</details>

<details>
<summary>🔗 Exemple de Configuration Hooks (Kiro uniquement, généré automatiquement)</summary>

Sauvegarde automatique en fin de session supprimée. Vérification du workflow de développement (`.kiro/hooks/dev-workflow-check.kiro.hook`) :

```json
{
  "enabled": true,
  "name": "Vérification du Workflow de Développement",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "Principes : vérifier avant d'agir, pas de tests à l'aveugle, ne marquer comme terminé qu'après réussite des tests"
  }
}
```

</details>

## 🇨🇳 Utilisateurs en Chine

Le modèle d'Embedding (~200Mo) est téléchargé automatiquement au premier lancement. Si c'est lent :

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

Ou ajouter env dans la configuration MCP :

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 Stack Technique

| Composant | Technologie |
|-----------|-----------|
| Runtime | Python >= 3.10 |
| BD Vectorielle | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| Tokenizer | HuggingFace Tokenizers |
| Protocole | Model Context Protocol (MCP) |
| Web | HTTPServer natif + Vanilla JS |

## 📋 Journal des Modifications

### v1.0.3

**Optimisation de la recherche recall**
- 🔍 `recall` ajoute le paramètre `tags_mode` : `any` (correspondance OR) / `all` (correspondance AND)
- 🔍 `query + tags` utilise OR par défaut (tout tag correspondant entre dans les candidats), résolvant les résultats manqués avec plusieurs tags
- 🔍 `tags` seul conserve AND (navigation précise par catégorie), rétrocompatible
- 📝 Règles de Steering mises à jour avec directives de recherche

### v0.2.8

**Tableau de Bord Web**
- 📋 Modal de détail des problèmes archivés : clic sur une carte archivée affiche les détails en lecture seule (tous les champs structurés : investigation/cause racine/solution/résultat de test/fichiers modifiés), bouton rouge de suppression en bas pour suppression permanente

**Renforcement des Règles Steering**
- 📝 `track create` exige désormais le champ `content` obligatoire (décrire les symptômes et le contexte du problème), interdit d'envoyer uniquement le titre
- 📝 `track update` post-investigation exige les champs `investigation` et `root_cause`
- 📝 `track update` post-correction exige les champs `solution`, `files_changed` et `test_result`
- 📝 Section 4 ajoute la sous-section "Normes de Remplissage des Champs" avec les champs obligatoires par étape
- 📝 Section 5 étendue de "Vérification de Modification de Code" à "Vérification Pré-Opération", ajout de la règle recall des enregistrements d'erreurs avant démarrage du tableau de bord/publication PyPI/redémarrage de service
- 📝 `install.py` STEERING_CONTENT synchronisé avec toutes les modifications

**Optimisation des Outils**
- 🔧 Description du champ `content` de l'outil `track` changée de "contenu d'investigation" à "description du problème (obligatoire lors du create)"

### v0.2.7

**Extraction automatique de mots-clés**
- 🔑 `remember`/`auto_save` extraient automatiquement les mots-clés du contenu pour compléter les tags — l'IA n'a plus besoin de transmettre manuellement des tags complets
- 🔑 Utilise la segmentation chinoise jieba + extraction par regex anglaise, extraction précise de mots-clés pour les contenus mixtes chinois-anglais
- 🔑 Nouvelle dépendance `jieba>=0.42`

### v0.2.6

**Restructuration des règles Steering**
- 📝 Document de règles Steering réécrit de l'ancienne structure à 3 sections vers 9 sections (Démarrage de session / Flux de traitement / Règles de blocage / Suivi des problèmes / Revue de code / Gestion des tâches Spec / Qualité de mémoire / Référence des outils / Standards de développement)
- 📝 Modèle STEERING_CONTENT de `install.py` synchronisé, les nouveaux projets obtiennent les règles mises à jour à l'installation
- 📝 Tags passés de listes fixes à extraction dynamique (mots-clés extraits du contenu), améliorant la précision de recherche de mémoire

**Corrections de bugs**
- 🐛 Outil `readme` `handle_readme()` manquait `**_`, causant l'erreur MCP `unexpected keyword argument 'engine'`
- 🐛 Correction de la pagination de recherche de mémoire du tableau de bord web (filtrage complet avant pagination avec requête de recherche, corrigeant les résultats incomplets)

**Mises à jour de documentation**
- 📖 Nombre d'outils README 7→8, diagramme d'architecture `digest`→`task`, descriptions d'outils `task`/`readme` ajoutées
- 📖 Paramètres `auto_save` mis à jour de `decisions[]/modifications[]/pitfalls[]/todos[]` vers `preferences[]/extra_tags[]`
- 📖 Exemple de règles Steering mis à jour du format 3 sections vers le résumé de structure 9 sections
- 📖 Mises à jour synchronisées sur 6 versions linguistiques

### v0.2.5

**Mode de développement piloté par les tâches**
- 🔗 Le suivi des problèmes (track) et la gestion des tâches (task) sont reliés par `feature_id` en un flux complet : découverte du problème → création de tâche → exécution → synchronisation automatique des statuts → archivage lié
- 🔄 `task update` synchronise automatiquement le statut du problème associé lors de la mise à jour (tout terminé→completed, en cours→in_progress)
- 📦 `track archive` archive automatiquement les tâches associées (liaison lors de l'archivage du dernier problème actif)
- 📦 Nouvelle action `archive` pour l'outil `task`, déplace toutes les tâches du groupe fonctionnel dans la table d'archives `tasks_archive`
- 📊 Les cartes de problèmes affichent la progression des tâches associées (ex. `5/10`), la page des tâches supporte le filtrage par archives

**Nouveaux outils**
- 🆕 Outil `task` — gestion des tâches (batch_create/update/list/delete/archive), sous-tâches arborescentes, lié aux documents spec via feature_id
- 🆕 Outil `readme` — génération automatique du contenu README depuis TOOL_DEFINITIONS/pyproject.toml, multilingue et comparaison de différences

**Améliorations des outils**
- 🔧 `track` : nouvelle action delete, 9 champs structurés (description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id), list par issue_id pour élément unique
- 🔧 `recall` : nouveau paramètre source (manual/auto_save) et mode brief (retourne uniquement content+tags, économise le contexte)
- 🔧 `auto_save` : marque les mémoires avec source="auto_save", distingue les mémoires manuelles des sauvegardes automatiques

**Refactorisation par séparation des tables de connaissances**
- 🗃️ project_memories + user_memories en tables indépendantes, élimine les requêtes mixtes scope/filter_dir, amélioration des performances
- 📊 DB Schema v4→v6 : issues ajoutent 9 champs structurés + tables tasks/tasks_archive + champ memories.source

**Tableau de bord Web**
- 📊 Page d'accueil avec carte d'état de blocage (rouge bloqué/vert normal), clic pour accéder à la page d'état de session
- 📊 Nouvelle page de gestion des tâches (groupes fonctionnels pliables, filtrage par statut, recherche, CRUD)
- 📊 Navigation latérale optimisée (état de session, problèmes, tâches remontés en position centrale)
- 📊 Liste des mémoires avec filtrage source et filtre d'exclusion exclude_tags

**Stabilité et normes**
- 🛡️ Boucle principale du serveur avec capture globale des exceptions, les erreurs de message unique ne font plus planter le serveur
- 🛡️ Couche Protocol avec saut de lignes vides et tolérance aux erreurs de parsing JSON
- 🕐 Horodatages changés d'UTC au fuseau horaire local
- 🧹 Nettoyage du code redondant (méthodes non appelées, imports redondants, fichiers de sauvegarde)
- 📝 Modèle Steering avec section workflow Spec et gestion des tâches, règles de continuation context transfer

### v0.2.4

- 🔇 Prompt du hook Stop changé en instruction directe, éliminant les réponses dupliquées de Claude Code
- 🛡️ Règles Steering auto_save avec protection court-circuit, ignore les autres règles en fin de session
- 🐛 Correction d'idempotence de `_copy_check_track_script` (retourner l'état de changement pour éviter les faux "synchronisé")
- 🐛 Correction d'incompatibilité `row.get()` dans issue_repo delete avec `sqlite3.Row` (utiliser `row.keys()`)
- 🐛 Correction du défilement de la page de sélection de projets du tableau de bord Web (impossible de défiler avec beaucoup de projets)
- 🐛 Correction de pollution CSS du tableau de bord Web (strReplace remplacement global a corrompu 6 sélecteurs de style)
- 🔄 Tous les dialogues confirm() du tableau de bord Web remplacés par modal showConfirm personnalisé (supprimer mémoire/issue/tag/projet)
- 🔄 Opérations de suppression du tableau de bord Web avec gestion des réponses d'erreur API (toast au lieu d'alert)
- 🧹 `.gitignore` ajoute la règle d'ignorance du répertoire legacy `.devmemory/`
- 🧪 Nettoyage automatique des résidus de projets temporaires pytest en DB (conftest.py session fixture)

### v0.2.3

- 🛡️ Hook PreToolUse : vérification obligatoire du track issue avant Edit/Write, rejet si aucun issue actif (Claude Code / Kiro / OpenCode)
- 🔌 Plugin OpenCode mis à niveau au format SDK `@opencode-ai/plugin` (hook tool.execute.before)
- 🔧 `run install` déploie automatiquement check_track.sh avec injection dynamique du chemin
- 🐛 Correction de l'incompatibilité `row.get()` avec `sqlite3.Row` dans issue_repo archive/delete
- 🐛 Correction de la condition de concurrence session_id : lecture de la dernière valeur depuis la DB avant incrémentation
- 🐛 Validation du format date de track (YYYY-MM-DD) + validation du type issue_id
- 🐛 Renforcement de l'analyse des requêtes Web API (validation Content-Length + limite 10MB + gestion des erreurs JSON)
- 🐛 Correction de la logique scope du filtre de tags (`filter_dir is not None` au lieu de vérification falsy)
- 🐛 Validation de la longueur des octets struct.unpack pour l'export des données vectorielles
- 🐛 Migration versionnée du schéma (table schema_version + migration incrémentielle v1/v2/v3)
- 🐛 Correction de la synchronisation du numéro de version `__init__.py`

### v0.2.2

- 🔇 Tableau de bord Web : paramètre `--quiet` pour supprimer les logs de requêtes
- 🔄 Tableau de bord Web : paramètre `--daemon` pour exécution en arrière-plan (macOS/Linux)
- 🔧 Correction de la génération de configuration MCP dans `run install` (sys.executable + champs complets)
- 📋 Suivi des problèmes CRUD et archivage (Tableau de bord Web ajout/édition/archivage/suppression + association mémoires)
- 👆 Clic n'importe où sur la ligne pour ouvrir le modal d'édition (mémoires/problèmes/tags)
- 🔒 Règles de blocage appliquées lors des continuations de session/transferts de contexte (reconfirmation requise)

### v0.2.1

- ➕ Ajouter des projets depuis le tableau de bord Web (navigateur de répertoires + saisie manuelle)
- 🏷️ Correction de la contamination des tags entre projets (opérations limitées au projet actuel + mémoires globales)
- 📐 Troncature par points de suspension dans la pagination des modales + largeur 80%
- 🔌 OpenCode install génère automatiquement le plugin auto_save (événement session.idle)
- 🔗 Claude Code / Cursor / Windsurf install génère automatiquement la configuration Hooks (sauvegarde automatique en fin de session)
- 🎯 Améliorations UX du tableau de bord Web (retour Toast, guides d'état vide, barre d'export/import)
- 🔧 Clic sur les cartes de statistiques (clic sur les compteurs mémoire/problèmes pour voir les détails)
- 🏷️ Page de gestion des tags distingue les sources projet/global (marqueurs 📁/🌐)
- 🏷️ Le nombre de tags des cartes projet inclut désormais les tags des mémoires globales

### v0.2.0

- 🔐 Authentification par Token du tableau de bord Web
- ⚡ Cache de vecteurs Embedding, pas de calcul redondant pour un contenu identique
- 🔍 recall supporte la recherche combinée query + tags
- 🗑️ forget supporte la suppression par lots (paramètre memory_ids)
- 📤 Export/import de mémoires (format JSON)
- 🔎 Recherche sémantique dans le tableau de bord Web
- 🗂️ Bouton de suppression de projet dans le tableau de bord Web
- 📊 Optimisation des performances du tableau de bord Web (élimination des analyses complètes de table)
- 🧠 Compression intelligente de digest
- 💾 Persistance de session_id
- 📏 Protection de limite de longueur de content
- 🏷️ Référence dynamique de version (plus codée en dur)

### v0.1.x

- Version initiale : 7 outils MCP, tableau de bord Web, visualisation 3D vectorielle, support multilingue

## License

Apache-2.0
