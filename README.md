# Loan Data Cleaning & Analysis using PySpark on Databricks
This project demonstrates a complete data cleaning and analysis workflow using PySpark on Databricks with a real-world loan dataset. This project focuses on data cleaning and exploration. Further steps like machine learning or pipeline deployment can be built on top of this clean dataset.
### Dataset - <a href = 'https://drive.google.com/file/d/1e6phh7Df8mzYoE-sBXPVJklnSt_wHwkq/view?usp=drive_link'>Dataset from kaggle</a>
The dataset used in this project is LoanStats_2018Q4.csv, which contains information about loan applications, including:
- Loan Amounts
- Loan Grades
- Interest Rates
- Employment Length
- Revolving Utilization
- Loan Status, etc.
### Tools & Technologies
- Databricks (PySpark Notebook)
- Apache Spark (PySpark DataFrames & Spark SQL)
- CSV File Format
### Project Workflow
1. Data Ingestion
- Loaded CSV dataset into a Spark DataFrame with schema inference and headers.

2. Data Exploration
- Inspected schema, descriptive statistics, and initial data quality checks.

3. Data Cleaning
Cleaned columns containing strings (like term and emp_length) by:
- Removing text like "months", "years", "%".
- Extracting numeric values using regex.
- Handled missing values using counts and filled averages where required.
- Created additional derived columns for clean analysis.

4. Data Analysis
- Performed covariance and correlation analysis between key numerical fields.
- Used crosstab to analyze relationships between loan grades and loan status (good/bad).
- Conducted frequency analysis on categorical fields like purpose and grade.
- Calculated approximate percentiles (approxQuantile) for numeric fields.

5. SQL Integration
- Created temporary SQL tables for advanced analysis using Spark SQL.
### Key Insights
- Identified distribution of good/bad loans across different grades.
- Detected missing data and handled them effectively.
- Standardized messy string fields for further analysis.
### Skills Demonstrated
- Data Cleaning & Preprocessing using PySpark
- Regex-Based Text Processing
- Exploratory Data Analysis
- Spark SQL Queries
- Working with Databricks Notebooks
### How to Run
- Upload the dataset to Databricks FileStore or DBFS.
- Import the notebook into your Databricks workspace.
- Attach notebook to a running cluster.
- Run all cells step by step.
