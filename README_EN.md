# **📊 Transaction Post-Analysis Framework**

**Version**: V1.0 | **Author**: Charles

### **🌐 Language**

[中文](README.md) | [**English**](README_EN.md)

## **📖 Introduction**

The **Transaction Post-Analysis Framework** is a data analysis solution based on MySQL and Power BI, designed to handle complex transaction data streams. Through a layered architecture (ODS \-\> DWD \-\> ADS), the system automates daily overviews, deep merchant analysis, risk monitoring, and monthly trend forecasting, enabling business teams to quickly identify transaction anomalies and growth opportunities.

## **🏗 Architecture**

The system follows a standardized data processing flow, from raw data ingestion to final visualization:

[![](https://mermaid.ink/img/pako:eNqdk1Fv2jAQx7-K5UrTJgFKCCHUD5MglBaplTqG-rCkD4YcwaqxI9sZUMR33yWhClqfNuflnPv_zvL_zie61hlQRnPDiy1ZTlNFcNly1fxI6ZQ7Tp6NXoO1QuVkJvU-pY2sWuNkwfdkabiyfO2EVqQiXkm3-51Mvv788XgNP_IjmG8tPKllcTJXDswOMsEdkHGeG8h5XWvJVxLsa0vENTFNZkJxSV4E7K-z0zp7lzzrPRgymaPAllyK97raJ-EseeIK82SMxY5WfJQClaXqkxOxNkBiXvCVkMIJsNc2WHeUQO7JRkjJbja3m451Rr8BuwmC4BJ39yJzW9YvDn9zD__Jzf-Za6P7pG7tC9qTNfaQbg9NGbeShyTWu0LCAa8t16VsevKFLIR9w9bkOfb0Qk1aat60s5qG30AWUGjj7EV2d2Uw7eDUiYyyDZcWOnSHI8CrPT1VqpS6LewgpQzDDDa8lK5y_IxcwdUvrXeUOVMiaXSZbz82ZYHXgang2LdWgSeCiXWpHGV-VFeg7EQPlAV9vxeN_H4w9Dx_GPhDzB4pi8LeaBCEI_yi2-Eg8s8d-l6f6fWi0BuM-uFwEHhhMAgQwNF12jw1b6l-Uuc_qs8FKQ?type=png)](https://mermaid-live.nodejs.cn/edit#pako:eNqdk1Fv2jAQx7-K5UrTJgFKCCHUD5MglBaplTqG-rCkD4YcwaqxI9sZUMR33yWhClqfNuflnPv_zvL_zie61hlQRnPDiy1ZTlNFcNly1fxI6ZQ7Tp6NXoO1QuVkJvU-pY2sWuNkwfdkabiyfO2EVqQiXkm3-51Mvv788XgNP_IjmG8tPKllcTJXDswOMsEdkHGeG8h5XWvJVxLsa0vENTFNZkJxSV4E7K-z0zp7lzzrPRgymaPAllyK97raJ-EseeIK82SMxY5WfJQClaXqkxOxNkBiXvCVkMIJsNc2WHeUQO7JRkjJbja3m451Rr8BuwmC4BJ39yJzW9YvDn9zD__Jzf-Za6P7pG7tC9qTNfaQbg9NGbeShyTWu0LCAa8t16VsevKFLIR9w9bkOfb0Qk1aat60s5qG30AWUGjj7EV2d2Uw7eDUiYyyDZcWOnSHI8CrPT1VqpS6LewgpQzDDDa8lK5y_IxcwdUvrXeUOVMiaXSZbz82ZYHXgang2LdWgSeCiXWpHGV-VFeg7EQPlAV9vxeN_H4w9Dx_GPhDzB4pi8LeaBCEI_yi2-Eg8s8d-l6f6fWi0BuM-uFwEHhhMAgQwNF12jw1b6l-Uuc_qs8FKQ)

## **🛠 Tech Stack**

| Component | Description |  
| --------- | ----------- |  
| Database | MySQL 9.5 |  
| SQL Standard | ANSI SQL (utilizing Window Functions for complex analytics) |  
| Visualization | Power BI (via ODBC integration) / Excel Export |  
| Scripting | Shell (Automation), Python (Data Transformation/Import) |

## **📦 Core Modules**

### **1\. Daily Overview ☀️**

* **Transaction Monitoring**: Real-time tracking of daily volume, success rates, and average duration.  
* **Ranking Analysis**: Dynamic generation of merchant transaction rankings.  
* **Random Sampling**: Automated sampling of anomalies (failure codes, timeouts, extreme amounts) from the last 24 hours.

### **2\. Deep Analysis 🔍**

* **Merchant Behavior**: In-depth analysis of specific merchant funnels and failure reasons.  
* **User Profiling**: Identification of user transaction patterns, risk scoring, and blacklist cross-validation.

### **3\. Monthly Trends 📈**

* **Trend Identification**: Analysis of long-term trends and seasonal patterns.  
* **Anomaly Detection**: Detection and alerting of abnormal data fluctuations.

### **4\. Power BI Integration 📊**

* **Interactive Dashboards**: Pre-defined data views supporting real-time refreshing and multi-dimensional drill-downs.

## **📂 Project Structure**

sql-transaction-analysis/  
├── .trae/documents/            \# 📄 Project Docs (Requirements & Architecture)  
├── sql/                        \# 🧠 Core SQL Scripts  
│   ├── daily\_overview/         \# \[Daily\] Overview, Trends, Sampling  
│   ├── deep\_analysis/          \# \[Deep\] Merchant & User Specific Analysis  
│   ├── monthly\_trends/         \# \[Monthly\] Long-term Trend Reports  
│   ├── power\_bi\_integration/   \# \[BI\] Views for Power BI  
│   └── setup/                  \# \[Setup\] Schema, Indexes, Sample Data  
├── transform_example.py        \# 📝 Automating Python script execution (importing data from an Excel spreadsheet into a MySQL database)  
└── README.md                   \# Documentation

## **🗃 Data Models**

The system follows data warehouse layering principles to ensure data clarity and traceability:

### **ODS Layer (Operational Data Store)**

* ods\_pagsmile\_orders\_raw: Raw order data.  
* ods\_transfersmile\_payouts\_raw: Raw payout data.

### **DWD Layer (Data Warehouse Detail)**

* dwd\_payin\_orders\_d: Standardized Pay-in table (enriched with status semantics, identity standardization, duration metrics).  
* dwd\_payout\_orders\_d: Standardized Payout table (enriched with fee calculations, arrival metrics).

## **🚀 Quick Start**

### **1\. Data Preparation**

Import raw data and configure the local path in the Python script:

python scripts/transform.py

### **2\. Environment Initialization**

Initialize exchange rate tables (if needed):

\-- Run scripts in sql/setup/  
SOURCE sql/setup/create\_tables.sql;  
SOURCE sql/setup/fx\_rates.sql; \-- Insert or import daily FX rates

### **3\. Build Views & Models**

Initialize compatible views and the DWD layer:

SOURCE sql/shared/compat\_views.sql;  
SOURCE sql/shared/dwd\_views.sql;

### **4\. Execute Analysis Tasks**

Run the corresponding SQL task scripts (Task 1 \- Task 8\) based on business needs:

* **Example:** Run sql/daily\_overview/task1\_overview\_report.sql for today's overview.  
* Results can be exported as .xlsx via MySQL Workbench.

### **5\. Power BI Visualization**

1. Execute scripts under sql/power\_bi\_integration/ to create BI-specific views.  
2. Open Power BI Desktop and configure the ODBC connection.  
3. Click **Refresh** to load the latest analysis data.

## **✅ Testing & Validation**

Run test scripts to ensure data integrity:

SOURCE tests/test\_queries.sql;  
SOURCE tests/test\_data\_validation.sql;

*Created by Charles*
