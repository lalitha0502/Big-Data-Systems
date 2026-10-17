# Problem 18: Retail Customer Segmentation & Churn Prediction

## Big Data Systems Project — Hadoop Ecosystem

This project implements an end-to-end batch analytics pipeline for retail customer segmentation and churn analysis using the Hadoop ecosystem.

The pipeline processes customer profiles, product catalog information, and purchase transaction history to calculate Recency, Frequency, and Monetary (RFM) metrics. These metrics are used to segment customers into behavioral groups, analyze customer churn, and store customer-level analytical features in Apache HBase for efficient retrieval.

The project demonstrates how multiple big data technologies can work together to transform raw transactional data into useful customer analytics.

---

## 1. Problem Statement

Retail businesses generate large volumes of customer and transaction data. Analyzing this information helps businesses understand customer purchasing behavior, identify valuable customers, and detect customers who may have stopped purchasing.

The objective of this project is to develop a batch-processing pipeline that:

- Processes customer, product, and purchase datasets.
- Computes customer-level RFM metrics.
- Segments customers based on their purchasing behavior.
- Identifies potentially churned customers using an `is_churned` label.
- Performs customer segmentation and cohort-based analysis using Hive.
- Stores customer features and segment information in HBase.
- Supports efficient retrieval of individual customer records.

---

## 2. Project Objectives

1. Integrate Hadoop Distributed File System (HDFS) for distributed data storage.
2. Implement RFM feature computation using Apache Pig and Hadoop processing.
3. Use Apache Hive for data warehousing, SQL analytics, customer segmentation, and churn analysis.
4. Store customer feature vectors in Apache HBase.
5. Automate the execution of the pipeline using shell scripts.
6. Validate the outputs at each processing stage.
7. Demonstrate the integration of batch analytics and NoSQL storage.

---

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Apache Hadoop HDFS | Distributed storage of input datasets and intermediate outputs |
| Hadoop MapReduce | Batch processing support for distributed computations |
| Apache Pig | Data transformation and RFM feature computation |
| Apache Hive | SQL-based analytics, segmentation, and cohort analysis |
| Apache HBase | Storage and retrieval of customer feature records |
| Python | Dataset generation and HBase ingestion preparation |
| Bash Shell | Pipeline orchestration and execution |
| CSV | Input data format |

### Why These Technologies?

- **HDFS:** Provides a shared storage layer for the pipeline.
- **Pig:** Provides a dataflow-oriented approach to customer feature engineering.
- **Hive:** Enables SQL-based analysis of customer behavior and segment statistics.
- **HBase:** Supports customer-keyed lookups without scanning the complete analytical dataset.
- **Python:** Automates synthetic data generation and prepares customer records for ingestion.
- **Bash:** Coordinates the different stages of the workflow.

---

## 4. Dataset Description

The project uses synthetic retail datasets generated for experimentation and demonstration.

| Dataset | Number of Records | Description |
|---|---:|---|
| `customers.csv` | 200 | Customer profiles and demographic information |
| `products.csv` | 50 | Product catalog, categories, and pricing information |
| `purchases.csv` | 1,500 | Customer purchase and transaction history |

### Dataset Files

- `customers.csv` contains customer identifiers and profile information.
- `products.csv` contains product identifiers and catalog attributes.
- `purchases.csv` contains purchase transactions used to calculate customer-level metrics.

The synthetic datasets make the pipeline reproducible without requiring access to confidential commercial transaction data.

**Note:** The current dataset is intended for project demonstration. Results should not be interpreted as representative of an actual retail business.

---

## 5. System Architecture

The pipeline follows a staged data-processing architecture.

```text
                  Synthetic Retail Datasets
               ┌────────────────────────────┐
               │  customers.csv             │
               │  products.csv              │
               │  purchases.csv             │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │          HDFS              │
               │ Distributed Data Storage   │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │ Apache Pig / MapReduce     │
               │ RFM Feature Computation    │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │       HDFS Output          │
               │ Customer RFM Features      │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │       Apache Hive          │
               │ Warehouse and SQL Queries  │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │ Customer Segmentation      │
               │ Churn and Cohort Analysis  │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │       Apache HBase         │
               │ customer_rfm_features      │
               └──────────────┬─────────────┘
                              │
                              ▼
               ┌────────────────────────────┐
               │ Customer Record Retrieval  │
               │ Get / Scan / Read / Write  │
               └────────────────────────────┘
```

### Pipeline Stages

**Stage 1 — Data Generation and Ingestion**

Generate the input datasets and upload them to HDFS.

**Stage 2 — RFM Computation**

Process transaction records to calculate customer recency, purchase frequency, and monetary value.

**Stage 3 — Hive Analytics**

Create queryable tables and analyze customer behavior, segmentation, and churn.

**Stage 4 — HBase Ingestion**

Load the resulting customer-level feature records into the `customer_rfm_features` table.

**Stage 5 — Verification**

Retrieve customer records and validate HBase read and write operations.

---

## 6. Directory Structure

```text
Assignment1/
│
├── Problem_18.docx
├── README.md
│
├── data/
│   ├── generate_datasets.py
│   ├── customers.csv
│   ├── purchases.csv
│   └── products.csv
│
└── scripts/
    ├── 01_hdfs_setup.sh
    ├── 02_rfm_computation.pig
    ├── 02_rfm_mapper.py
    ├── 02_rfm_reducer.py
    ├── 03_hive_analysis.hql
    ├── 04_hbase_setup.hb
    ├── 04_hbase_load.py
    ├── 04_hbase_queries.sh
    └── run_all_pipeline.sh
```

### Important Scripts

| Script | Responsibility |
|---|---|
| `generate_datasets.py` | Generates the synthetic retail datasets |
| `01_hdfs_setup.sh` | Creates HDFS directories and uploads input data |
| `02_rfm_computation.pig` | Computes customer-level RFM features |
| `02_rfm_mapper.py` | Provides an alternative Hadoop Streaming mapper |
| `02_rfm_reducer.py` | Provides an alternative Hadoop Streaming reducer |
| `03_hive_analysis.hql` | Creates Hive tables and performs customer analytics |
| `04_hbase_setup.hb` | Configures the HBase table |
| `04_hbase_load.py` | Prepares customer records for HBase ingestion |
| `04_hbase_queries.sh` | Demonstrates customer lookups and table scans |
| `run_all_pipeline.sh` | Coordinates the end-to-end pipeline |

---

## 7. Prerequisites

Ensure that the following components are installed and configured:

- Java compatible with the installed Hadoop ecosystem components.
- Apache Hadoop with HDFS and the required processing services.
- Apache Pig.
- Apache Hive.
- Apache HBase.
- Python 3.
- Bash shell.

The Hadoop services, environment variables, configuration files, and required ports must be available before running the pipeline.

Verify the Java installation:

```bash
java -version
```

Verify Hadoop:

```bash
hadoop version
```

Verify HDFS:

```bash
hdfs dfs -ls /
```

Check running Java processes:

```bash
jps
```

Verify that the Pig, Hive, and HBase commands are available:

```bash
pig -version
hive --version
hbase version
```

Use the versions compatible with your configured environment rather than assuming that every version works with every other component.

---

## 8. Installation and Execution

### Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd Assignment1
```

Replace the repository URL with the actual GitHub repository URL.

If you already have the project open in VS Code, you can skip cloning.

### Step 2: Generate the Datasets

```bash
cd data
python3 generate_datasets.py
```

This generates the customer, product, and purchase CSV files.

Return to the project root:

```bash
cd ..
```

### Step 3: Configure HDFS and Upload Data

```bash
cd scripts
bash 01_hdfs_setup.sh
```

This stage creates the required HDFS directories and uploads the datasets.

Verify the uploaded data:

```bash
hdfs dfs -ls /data/retail
```

If your script uses different HDFS directory names, inspect `01_hdfs_setup.sh` and use the paths defined there.

### Step 4: Execute RFM Computation

```bash
pig -x mapreduce 02_rfm_computation.pig
```

This stage calculates customer-level RFM features and writes the resulting records to the configured HDFS output directory.

Inspect the output directory after the job finishes.

For example:

```bash
hdfs dfs -ls /data/retail/output/rfm_features
```

Use the actual output path defined in your Pig script.

### Step 5: Run Hive Analytics

```bash
hive -f 03_hive_analysis.hql
```

The Hive script creates the required analytical tables and executes customer segmentation and churn analysis queries.

Check the tables and results using the queries defined in the script.

### Step 6: Configure HBase

```bash
hbase shell 04_hbase_setup.hb
```

This creates or configures the HBase table used to store customer feature records.

The project uses:

```text
customer_rfm_features
```

### Step 7: Prepare and Load Customer Features

Generate the HBase ingestion statements:

```bash
python3 04_hbase_load.py
```

Execute the generated HBase shell commands using the input file and command specified by your ingestion script.

For example, if the script generates `load_hbase.hb` in the current directory:

```bash
hbase shell load_hbase.hb
```

Ensure that the generated file exists before running the command.

### Step 8: Verify the Results

```bash
bash 04_hbase_queries.sh
```

This demonstrates customer-level retrieval and table inspection.

The supplied project demonstration includes retrieving customer `CUST_0001` and verifying HBase read/write functionality.

---

## 9. Running the Complete Pipeline

The repository provides a master script for coordinating the processing stages.

From the `scripts/` directory, execute:

```bash
bash run_all_pipeline.sh
```

The script is intended to automate the pipeline from data ingestion through HBase verification.

Before execution, verify that:

1. The required Hadoop services are running.
2. The input CSV files exist.
3. The required environment variables are configured.
4. Pig, Hive, and HBase are accessible.
5. The paths and script dependencies match your local environment.

If a stage fails, inspect its error output and execute the relevant stage separately to identify the problem.

**Important:** The master script and individual commands should not be executed simultaneously. Use either the automated workflow or the step-by-step procedure.

---

## 10. RFM Metrics

RFM analysis summarizes customer purchasing behavior through three metrics.

### Recency

Recency measures how recently a customer made a purchase.

A lower recency value generally indicates more recent purchasing activity.

### Frequency

Frequency measures how often a customer has purchased during the observation period.

A higher frequency value indicates more frequent purchasing activity.

### Monetary Value

Monetary value measures the total purchase amount associated with a customer during the observation period.

A higher monetary value indicates greater historical spending.

### Customer Segmentation

The project uses customer-level RFM information to categorize customers into behavioral segments.

The segments observed in the supplied project output include:

- Champions
- Loyal Customers
- Potential Loyalists
- Needs Attention
- At Risk
- Lost / Churned

The exact segmentation rules depend on the thresholds and conditions implemented in the Hive analysis script.

---

## 11. Churn and Cohort Analysis

The pipeline assigns an `is_churned` label according to the churn rule implemented in the project.

This label allows the analytical queries to identify customers classified as churned and compare churn rates across customer segments.

Cohort analysis can be used to examine customer retention across groups defined by a shared starting period or other cohort criteria, depending on the implemented Hive queries.

The resulting analysis helps identify:

- Segments with higher observed churn.
- Customers requiring additional attention.
- Differences in purchasing recency and spending.
- Patterns in customer retention.

Because the project uses synthetic data, these findings are intended to demonstrate the analytics workflow rather than establish real-world customer behavior.

---

## 12. Results and Validation

The project demonstration provides evidence of the following outcomes.

| Component | Demonstrated Result |
|---|---|
| Dataset generation | Generated customer, product, and purchase datasets |
| HDFS | Data ingestion and HDFS directory operations |
| Pig | RFM feature computation workflow |
| Hive | Analytical tables and customer segment summaries |
| RFM output | Customer-level recency, frequency, and monetary values |
| HBase | `customer_rfm_features` table availability |
| Customer retrieval | Retrieved the record for `CUST_0001` |
| Read/write verification | Demonstrated storing and retrieving a test value |

The Hive summary output in the supplied project evidence reports six customer segments and their customer counts, churn rates, and average RFM-related values.

The HBase demonstration shows `CUST_0001` assigned to the Champions segment.

Refer to the final project report and execution screenshots for the corresponding terminal outputs.

No predictive-model accuracy is claimed in this project description because the demonstrated workflow focuses on batch analytics, RFM segmentation, churn labeling, and feature storage rather than evaluating a trained churn prediction model.

---

## 13. Challenges Encountered

The implementation involves several practical challenges when integrating multiple big data tools.

### Environment Configuration

Hadoop, Pig, Hive, and HBase require compatible configurations and accessible command-line tools.

**Approach:** Verify installations, environment variables, service status, and configuration files before running the pipeline.

### HDFS Path Management

Processing stages must read the files written by earlier stages.

**Approach:** Inspect HDFS directories and confirm output paths after each stage.

### Schema and Data Consistency

CSV columns, Hive table definitions, and derived feature records must use consistent field definitions.

**Approach:** Inspect headers, sample records, table schemas, and query results.

### Pipeline Debugging

A failure in one stage can prevent later stages from executing.

**Approach:** Run the pipeline incrementally, inspect error messages, and verify intermediate outputs before retrying.

### HBase Ingestion and Retrieval

Customer records must be written with the expected row keys and column qualifiers.

**Approach:** Verify the table schema and test point lookups after ingestion.

---

## 14. Limitations and Future Enhancements

### Current Limitations

- The current datasets are synthetic and relatively small.
- Segmentation results depend on the implemented RFM thresholds.
- Churn labels depend on the configured business rule.
- The workflow is batch-oriented rather than a continuous real-time analytics pipeline.
- The current demonstration does not establish predictive-model accuracy or business impact.

### Future Enhancements

Potential improvements include:

- Processing larger real-world transaction datasets.
- Automating periodic data ingestion and segmentation.
- Developing an interactive customer analytics dashboard.
- Integrating a trained churn prediction model and evaluating its performance.
- Adding incremental updates to customer features.
- Developing an API that retrieves HBase features for downstream applications.
- Adding automated data-quality checks and pipeline monitoring.

---

## 15. Project Deliverables

The project deliverables include:

1. Source code for dataset generation and processing.
2. Pig scripts for RFM feature computation.
3. Hive scripts for analytics and segmentation.
4. HBase setup, ingestion, and query scripts.
5. The end-to-end pipeline orchestration script.
6. The final project report with architecture, results, challenges, and screenshots.
7. Individual contribution details for all team members.

---

## 16. Team Contributions

Complete the following table according to each member's actual work.

| Team Member | Contribution |
|---|---|
| Member 1 | [Add name, roll number, and actual contribution] |
| Member 2 | [Add name, roll number, and actual contribution] |
| Member 3 | [Add name, roll number, and actual contribution] |

All team members should verify their individual contributions before submission.

---

## 17. Conclusion

This project demonstrates an integrated retail customer analytics pipeline using the Hadoop ecosystem.

HDFS provides distributed storage, Pig performs RFM feature engineering, Hive supports customer segmentation and churn analysis, and HBase stores customer-level analytical features for efficient retrieval.

The project provides practical experience in batch data processing, SQL analytics, customer behavior analysis, NoSQL storage, and multi-stage pipeline integration.

By combining these technologies, the workflow transforms raw retail transaction records into structured customer features and actionable segment summaries.

---

**Project:** Problem 18 — Retail Customer Segmentation & Churn Prediction  
**Course:** Big Data Systems / Big Data & Large-Scale Computing  
**Repository:** Add your GitHub repository URL here
