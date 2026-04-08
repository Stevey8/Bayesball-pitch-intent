# Bayesball: Modeling Pitch Intent Using a Bayesian Framework

[![Project Status](https://img.shields.io/badge/status-active-green.svg)](#)

*This is the Pitching-focused version of the Bayesball project. For the original batting-focused repository, please visit [Bayesball (Batting)](https://github.com/Stevey8/Bayesball).*

## Overview
Inspired by presentations at the **2026 SABR Analytics Conference**, this project addresses a critical gap in baseball analytics: the measurement of **pitch intent**. 

While intent is central to how pitching is taught and evaluated, it remains an unobserved (latent) variable in standard datasets. Current metrics often conflate a pitcher's decision-making with their physical execution. This project develops a quantitative framework to infer intended pitch locations and decompose every pitch outcome into three distinct components: **Strategy**, **Execution**, and **Randomness**.

---

## Technical Stack & Data Pipeline
The architecture is designed to handle high-volume, pitch-level Statcast data (7.4 million pitches from 2015 to 2025 seasons) using a local OLAP setup.

### Data Architecture
* **Sources:** Statcast pitch-level data via `pybaseball` & **MLB API** metadata.
* **Storage:** **Parquet** with **Hive-style partitioning** (`/year=/month=/`).
* **Database:** **DuckDB** for direct SQL analytical queries.

### Core ETL Scripts
* `download_statcast.py`: Automated retrieval and cleaning.
* `download_mlbapi.py`: Player bios and game context.
* `db.py`: Relational schema management.

> **[PLACEHOLDER: Architecture Diagram]**
> *Future addition: Mermaid.js / LucidChart diagram showing the flow from Data source -> Parquet -> DuckDB -> Model.*


### Overall Script Features 
- logging for easy debugging
- parquet storage: columnar storage to minimize I/O for multi season analysis, and hive style partitioning for ...
- ...


---

## Methodology

### 1. Data Engineering & Schema
*see the Technical Stack & Data Pipeline section above*

### 2. Bayesian Inference (Work in Progress)
We model the intended location $\theta$ as a latent variable. 
- **Prior:** 
- **Likelihood:** Probability of observed location $(x, z)$ given intent $\theta$ and pitcher command variance $\sigma^2$.

> **[PLACEHOLDER: Mathematical Deep Dive]**
> *Future addition: Link to Jupyter Notebook / LaTeX PDF explaining the MCMC sampling process and prior distributions.*

---

## Future Enhancements
- [ ] **Database Migration:** Moving from DuckDB to **PostgreSQL** for enterprise relational modeling.
- [ ] **Orchestration:** Integrating **Apache Airflow** for automated daily data refreshes.
- [ ] **Cloud:** Migrating storage to **GCP** for larger files like images and footage for future Computer Vision integration.
- [ ] **Validation:** Comparing inferred intent against "Catcher Setup" data (where available).

---

## Installation & Usage
*(Instructions will be updated as the CLI matures)*
```bash
# Example: Syncing data for the 2025 season
python download_statcast.py --season 2025
