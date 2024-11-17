# Lab 2: End-to-End Stock Market Data Analytics using Snowflake, Airflow, dbt, and a BI Tool-Preset

This repository contains the code and documentation for **Lab 2: Building an End-to-End Stock Market Data Analytics Pipeline**, focusing on ELT processes and data visualization. The project integrates **Airflow**, **Snowflake**, **dbt**, and a BI tool ( Preset) to extract, load, and transform stock market data and visualize actionable insights.

---

## Table of Contents
1. [Project Overview](#project-overview)  
2. [Architecture](#architecture)  
3. [Dataset](#dataset)  
4. [Components and Workflow](#components-and-workflow)  
5. [Setup and Usage](#setup-and-usage)  
6. [Results and Screenshots](#results-and-screenshots)  
7. [Repository Structure](#repository-structure)  
8. [Acknowledgments](#acknowledgments)  

---

## Project Overview
This project processes stock market data to create an analytics pipeline.  
**Key Features:**
- **ETL:** Populate raw data in Snowflake using Airflow.
- **ELT:** Transform raw data using dbt, including key metrics like:
  - 7-day moving average.
  - Relative Strength Index (RSI).
  - Price Momentum
- **Visualization:** Present insights using Tableau, Superset, or Preset.

**Use Case:** Stock Price Analytics for companies (e.g., ISRG, NFLX) to analyze trends, momentum, and performance.

---

## Architecture
The project follows a modular design for clarity and scalability.  
**High-Level System Diagram:**
1. **Data Extraction:** Fetch stock data from Alpha Vantage API.  
2. **ETL in Airflow:** Load data into Snowflake's `RAW_DATA.LAB2` table.  
3. **ELT in dbt:** Transform raw data to calculate key metrics in `ANALYTICS` schema.  
4. **Visualization:** Build interactive dashboards for insights.  

---

## Dataset
- **Source:** [Alpha Vantage API](https://www.alphavantage.co/).  
- **Details:** Time-series daily stock data for selected companies.  
- **Table:** `RAW_DATA.LAB2` with the following columns:
  - `symbol`: Stock symbol (e.g., NFLX, ISRG).
  - `date`: Date of the record.
  - `close`: Closing price for the day.

---

## Components and Workflow

### 1. Airflow (ETL)
- **Purpose:** Extract raw data from Alpha Vantage and load it into Snowflake.  
- **DAG Name:** `LAB2`  
- **Tasks:**
  - `extract`: Fetch stock data for the last 90 days.
  - `transform`: Prepare data for loading.
  - `load`: Insert data into Snowflake using idempotent transactions.
- **Variables and Connections:**  
  - `Alpha_url` and `vantage_api_key` for API configuration.  
  - Snowflake credentials (`snowflake_account`, `snowflake_username`, `snowflake_password`).  

### 2. dbt (ELT)
- **Purpose:** Perform transformations and create analytics models.  
- **Project Structure:**
  - `models/input`: Defines raw data sources.
  - `models/output`: Contains analytics models (e.g., 7-day moving average, RSI).
- **Commands:**
  - `dbt run`: Execute transformations.
  - `dbt test`: Validate transformations with schema tests.  

### 3. Visualization
- **Tool:** Tableau, Superset, or Preset.  
- **Key Metrics Visualized:**
  - Moving averages.
  - RSI.
  - Price momentum.
- **Features:** Interactive filters (e.g., date range).

---

## Setup and Usage

### Prerequisites
1. Python 3.9+  
2. Airflow with `apache-airflow-providers-snowflake` installed.  
3. Snowflake account.  
4. dbt Core installed.  
5. BI tool (ePreset).
6. Docker

### Steps
1. **Clone the repository:**  
   ```bash
   git clone <repo-url>
   cd <repo-folder>
   ```

2. **Configure Airflow Variables:**  
   Set the following variables in Airflow:
   - `Alpha_url`
   - `vantage_api_key`
   - `snowflake_account`
   - `snowflake_username`
   - `snowflake_password`

3. **Run Airflow DAG:**  
   Start the Airflow scheduler and trigger the `LAB2` DAG.

4. **Run dbt Models:**  
   Execute dbt transformations using:
   ```bash
   dbt run
   dbt test
   dbt snapshot
   ```

5. **Visualize Insights:**  
   Use Preset to load and analyze data from the Snowflake Stock_analytics table.

<img width="658" alt="Screenshot 2024-11-14 at 1 40 21 PM" src="https://github.com/user-attachments/assets/987aa873-3271-41fc-836b-3c9b2e41c0eb">

---

## Results and Screenshots
### 1. Airflow
- Screenshot of the `LAB2` DAG on Airflow Web UI.
<img width="1440" alt="image" src="https://github.com/user-attachments/assets/824008e4-9084-407d-9a3f-5006f5f633c5">

### 2. Data extraction
- Screenshots of ETL dag (`extract`, `transform`, `load`) and output logs.
  <img width="1437" alt="image" src="https://github.com/user-attachments/assets/7162eee6-b333-485b-8710-a167c4991e02">
  <img width="1440" alt="image" src="https://github.com/user-attachments/assets/4e3f1c20-ff75-4ab5-b542-a1e0bff5c076">


### 3. dbt
- Screenshots of dbt commands (`run`, `test`, `snapshot`) and output logs.
<img width="1440" alt="image" src="https://github.com/user-attachments/assets/64bf1574-0d63-4f2e-8edd-a4945e119538">
<img width="1439" alt="image" src="https://github.com/user-attachments/assets/36cbe5c9-758f-4dcb-814a-ead16c1d7d9a">

### 4. BI Tool
- Dashboard screenshots showcasing insights of RSI Calculation, Price Momentum and Moving Averages.
<img width="1055" alt="image" src="https://github.com/user-attachments/assets/622d66dc-7583-4fc7-88e9-f9f8ef99aa3a">

---

## Repository Structure
```plaintext
data226-lab2/
├── airflow/
│   ├── dags/
│   │   └── etl_airflow_lab02.py  # ETL-Airflow DAG
│   │   └── build_elt_with_dbt.py  # ELT-Airflow DAG
│   ├── variables.json   # Airflow variables for configuration
│   └── connections.json # Airflow Snowflake connection
│   ├── lab2/
│   │   └── ── models/
│   │   ├── input/
│   │   │   └── stock_abstract_view.sql
│   │   ├── output/
│   │   │   └── stock_analytics.sql  # Analytics transformations
│   │   ├── schema.yml
│   │   ├── schema.yml
│   │   └── ── dbt_project.yml   # dbt configuration
├── visualizations/
│   └── preset_dashboard.png
├── README.md  # Project documentation
└── requirements.txt  # Python dependencies

```

---

## Acknowledgments
- **San Jose State University (SJSU):** This project is part of the Data Analytics Lab series.
- **Professor Keyung Haan:** T For guidance and support throughout the class and lab.
- **Alpha Vantage API:** For providing stock market data.  
- **Tools Used:** Airflow, Snowflake, dbt, Preset, Docker.
