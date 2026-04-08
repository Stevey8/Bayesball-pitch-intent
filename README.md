# Bayesball: Modeling Pitch Intent Using a Bayesian Framework

[![Project Status](https://img.shields.io/badge/status-active-green.svg)](#)

*This is the Pitching-focused version of the Bayesball project. For the original batting-focused repository, please visit [Bayesball (Batting)](https://github.com/Stevey8/Bayesball).*

## Overview
Inspired by presentations at the **2026 SABR Analytics Conference**, this project addresses a critical gap in baseball analytics: the measurement of **pitch intent**. 

While intent is central to how pitching is taught and evaluated, it remains an unobserved (latent) variable in standard datasets. Current metrics often conflate a pitcher's decision-making with their physical execution. This project develops a quantitative framework to infer intended pitch locations and decompose every pitch outcome into three distinct components: **Strategy**, **Execution**, and **Randomness**.

---

## Data Pipeline
The system is designed to handle high-volume, pitch-level **Statcast** data (~7.4 million pitches from 2015 to 2025) using a local OLAP setup.

### Data Architecture
* **Sources:** Statcast pitch-level data via `pybaseball`, supplemented MLB API metadata.
* **Storage:** **Apache Parquet** with Hive-style partitioning (`/year=/month=/`). 
* **Database:** **DuckDB** for direct SQL queries on Parquet files.


### ETL Scripts
* `download_statcast.py`: ingestiong and cleaning.
* `download_mlbapi.py`: metadata enrichment.
* `db.py`: Relational schema management.

> **[PLACEHOLDER: Architecture Diagram]**
> *Future addition: Mermaid.js / LucidChart diagram showing the flow from Data source -> Parquet -> DuckDB -> Model.*


### Overall Script Features 
* **Columnar storage** with Parquet to enable efficient queries by readign only required columns.
* **Partitioning** (year/month) reduces data scanned via partition pruning.
* **Logging** supports pipeline monitoring and debugging 


---

## Methodology (WIP)

### 1. Problem Framing 
*outline*
- pitch intent as latent variable
- decomposition into strategy, execution, and randeomness or uncontrollable factors

### 2. Data Preparation and EDA
*outline*
- matchup of different handedness (LHP/RHP vs LHH/RHH)
- pitch type / movement profile context
- pitch sequencing and count context
- game context (scores, innings, runner on base, etc.)
- proba distribution / heat map of different stratification 

### 3. Modeling 
*outline*
- model pitch intent (intended location of a pitch) $\theta$ as a latent variable 
- Bayesian framework
  - prior: pitch type, proba distribuion of pitch landing location, etc.
  - likelihood: probability of observed location (`plate_x` and `plate_z`) given intent $\theta$ and pitcher command variace $\sigma^2$


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
