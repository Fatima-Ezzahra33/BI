# Entrepôt de Données & Analyse BI avec Power BI

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=flat&logo=microsoft-sql-server&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Architecture](https://img.shields.io/badge/Architecture-Médaillon-blue?style=flat)
![Statut](https://img.shields.io/badge/Statut-Complété-brightgreen?style=flat)

> Projet universitaire — Ingénierie des données & Business Intelligence  
> **Auteure :** Abdessettar Fatima-Ezzahra

---

## Contexte & Problématique

Dans un contexte où les organisations génèrent des volumes croissants de données hétérogènes issues de systèmes distincts (CRM, ERP), la consolidation et l'exploitation de ces données constituent un enjeu stratégique majeur pour la prise de décision.

Ce projet propose une implémentation complète d'un **entrepôt de données moderne** — de l'ingestion des données brutes jusqu'à la visualisation analytique — en s'appuyant sur l'**Architecture Médaillon** (Bronze / Argent / Or). Cette approche, préférée à des modèles alternatifs comme le Data Vault ou le schéma en flocon de neige, a été retenue pour sa lisibilité, sa modularité et son alignement avec les pratiques actuelles de l'industrie.

---

## Architecture des Données

![Architecture Médaillon](docs/architecture_entrepot_donnees_fr.png)

Le projet repose sur une organisation en trois couches progressives :

| Couche | Rôle | Sources |
|--------|------|---------|
| **Bronze** | Ingestion des données brutes sans transformation | CRM : `cust_info.csv`, `prd_info.csv`, `sales_details.csv` / ERP : `CUST_AZ12.csv`, `LOC_A101.csv`, `PX_CAT_G1V2.csv` |
| **Argent** | Nettoyage, déduplication, standardisation, enrichissement | Vues Silver issues des tables Bronze |
| **Or** | Modèle en étoile optimisé pour l'analyse décisionnelle | Dimensions clients/produits, table de faits ventes |

> Voir également : [`flux_donnees_medallion.drawio`](docs/flux_donnees_medallion.drawio), [`medallion.drawio`](docs/medallion.drawio), [`star_schema2.png`](docs/star_schema2.png)

---

## Objectifs du Projet

1. **Pipeline ETL complet** : Conception et implémentation de procédures stockées couvrant les trois couches de l'entrepôt.
2. **Analyse exploratoire (EDA)** : Profiling des données, détection des anomalies, distribution des variables clés.
3. **Analyses BI avancées** : Segmentation clients (RFM), évolution temporelle des ventes, performance par catégorie de produits.
4. **Qualité des données** : Scripts de validation et tests d'intégrité à chaque étape du pipeline.
5. **Visualisation** : Tableaux de bord interactifs sous Power BI pour le reporting décisionnel.

---

## Environnement Technique

| Composant | Outil / Technologie |
|-----------|---------------------|
| Base de données | SQL Server Express |
| Interface SQL | SQL Server Management Studio (SSMS) |
| Visualisation | Microsoft Power BI |
| Modélisation | Draw.io (`.drawio`) |
| Versionning | Git / GitHub |

**Jeux de données :** [`datasets/crm/`](datasets/crm/) et [`datasets/erp/`](datasets/erp/)  
**Catalogue de données :** [`docs/data_catalog.md`](docs/data_catalog.md)

---

## Implémentation Technique

### Ingénierie des Données (ETL)

| Étape | Script(s) |
|-------|-----------|
| Initialisation de la base | [`init_db.sql`](scripts/dataWarehouse/init_db.sql) |
| Bronze — DDL | [`ddl_bronze.sql`](scripts/dataWarehouse/bronze/ddl_bronze.sql) |
| Bronze — Chargement | [`proc_load_bronze.sql`](scripts/dataWarehouse/bronze/proc_load_bronze.sql) |
| Silver — DDL | [`ddl_silver.sql`](scripts/dataWarehouse/silver/ddl_silver.sql) |
| Silver — Transformation | [`proc_load_silver.sql`](scripts/dataWarehouse/silver/proc_load_silver.sql) |
| Gold — DDL | [`ddl_gold.sql`](scripts/dataWarehouse/gold/ddl_gold.sql) |
| Gold — Agrégations | [`procedures.sql`](scripts/dataWarehouse/gold/procedures.sql) |

### Analyses BI

Les analyses sont organisées en deux niveaux :

**Exploration (EDA)** — [`scripts/data_analysis/EDA/`](scripts/data_analysis/EDA/)  
Profiling initial, distributions, valeurs manquantes, détection d'outliers.

**Analyses avancées** — [`scripts/data_analysis/Advanced/`](scripts/data_analysis/Advanced/)

| Analyse | Fichier |
|---------|---------|
| Évolution temporelle des ventes | [`01_change_over_time_analysis.sql`](scripts/data_analysis/Advanced/01_change_over_time_analysis.sql) |
| Rapport clients (segmentation, RFM) | [`06_report_customers.sql`](scripts/data_analysis/Advanced/06_report_customers.sql) |
| *(autres analyses)* | `02` à `05`, `07`... |

### Tests & Validation

Tests de qualité des données à chaque étape du pipeline : [`tests/test_loading_proc.sql`](tests/test_loading_proc.sql)

---

## Compétences Mobilisées

- Modélisation dimensionnelle (schéma en étoile, grain de fait, dimensions conformées)
- Conception et optimisation de pipelines ETL en SQL procédural
- Nettoyage et standardisation de données multi-sources hétérogènes
- Analyse exploratoire et segmentation client (RFM)
- Construction de tableaux de bord décisionnels sous Power BI
- Mise en place de tests de qualité et validation des données

---

## Structure du Projet

```
BI/
├── README.md                        # Ce fichier
├── TODO.md                          # Suivi des tâches
├── datasets/
│   ├── crm/                         # Clients, produits, ventes (CRM)
│   └── erp/                         # Clients, localisations, catégories (ERP)
├── docs/
│   ├── architecture_entrepot_donnees_fr.png
│   ├── data_catalog.md              # Catalogue des données
│   ├── star_schema2.png             # Schéma en étoile
│   └── *.drawio                     # Diagrammes de flux et modèles
├── scripts/
│   ├── dataWarehouse/
│   │   ├── init_db.sql
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── data_analysis/
│       ├── EDA/                     # Exploration initiale
│       └── Advanced/                # Analyses BI approfondies
└── tests/
    └── test_loading_proc.sql        # Tests qualité
```

---

## Références

- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd ed. Wiley.
- Inmon, W.H. (2005). *Building the Data Warehouse*, 4th ed. Wiley.
- Microsoft. (2024). *Medallion Architecture — Azure Databricks Documentation*. [lien](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)