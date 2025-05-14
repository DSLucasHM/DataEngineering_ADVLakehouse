# Azure Data Factory and Databricks ETL Project

---

This project outlines an ETL (Extract, Transform, Load) pipeline built using Azure Data Factory and Azure Databricks to process sales data. It follows a medallion architecture (Bronze, Silver, Gold layers) to refine raw data into an analytics-ready format.



### 👨‍💻 Author

---

**Lucas Miyazawa**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/lucasmiyazawa/) [![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:lucasmiyazawa@icloud.com)



### 📚 About the Project

---

This project implements a data engineering pipeline using Azure Data Factory (ADF) and Azure Databricks. The primary goal is to process raw data from a landing zone, transform it through several layers (Bronze, Silver), and finally load it into a data mart consisting of dimension and fact tables (Gold layer) for analytical purposes. The pipeline leverages Databricks notebooks for data ingestion, transformation, and loading, utilizing Delta Lake for robust and reliable data storage.

The project follows a medallion architecture:

*   **Bronze Layer:** Ingests raw source data from parquet files into Delta tables, adding ingestion timestamps. This layer provides a versioned, raw copy of the source data.
*   **Silver Layer:** Cleans, conforms, and enriches the data from the Bronze layer. Business rules are applied here to prepare the data for analytics. This includes handling duplicates, applying specific transformations based on business requirements (e.g., grouping South American addresses, unifying company names), and anonymizing sensitive information.
*   **Gold Layer (Data Mart):** Creates aggregated and business-centric dimension and fact tables from the Silver layer data. These tables are optimized for reporting and analytical queries.

Azure Data Factory is intended to orchestrate the execution of these Databricks notebooks in a production environment, though the provided notebooks include notes about simulated execution for development and testing purposes. The project emphasizes data quality, schema validation, and includes utility functions for common tasks like duplicate checking, deduplication, and upserting data into Delta tables.


### 🗂️ Project Structure

---

The project is organized into several Databricks notebooks, categorized by their role in the data pipeline (Bronze, Silver, Gold/Data Mart layers) and utility functions. Azure Data Factory would typically be used to orchestrate these notebooks.

```
/databricks_notebooks/
  ├── Bronze/
  │   ├── Bronze_Config.ipynb                 # Configuration for Bronze layer ingestion (file paths, table names)
  │   ├── Bronze_Ingestion.ipynb              # Main notebook for ingesting data into Bronze Delta tables
  │   └── Bronze_Ingestion (Not in use, With Loop).ipynb # Alternative ingestion (not currently used)
  ├── Silver/
  │   ├── Silver_Saleslt_Address.ipynb          # Cleans and transforms Address data
  │   ├── Silver_Saleslt_Customer.ipynb         # Cleans and transforms Customer data
  │   ├── Silver_Saleslt_Product.ipynb          # Cleans and transforms Product data
  │   ├── Silver_Saleslt_ProductCategory.ipynb  # Cleans and transforms ProductCategory data
  │   ├── Silver_SalesLT_ProductDescription.ipynb # Cleans and transforms ProductDescription data
  │   ├── Silver_SalesLT_ProductModel.ipynb     # Cleans and transforms ProductModel data
  │   ├── Silver_SalesLT_ProductModelDescription.ipynb # Cleans and transforms ProductModelDescription data
  │   ├── Silver_SalesLT_SalesOrderDetail.ipynb # Cleans and transforms SalesOrderDetail data
  │   ├── Silver_SalesLT_SalesOrderHeader.ipynb # Cleans and transforms SalesOrderHeader data
  ├── Gold_DataMart/
  │   ├── dim_SalesLT_Address.ipynb             # Creates Address dimension table
  │   ├── dim_SalesLT_Customer.ipynb            # Creates Customer dimension table
  │   ├── dim_SalesLT_Product.ipynb             # Creates Product dimension table
  │   ├── f_SalesLT_OrderDetail.ipynb           # Creates OrderDetail fact table
  │   ├── f_SalesLT_OrderHeader.ipynb           # Creates OrderHeader fact table
  └── Utils/
      └── Utils.ipynb                         # Utility functions for data processing (e.g., duplicate checks, upserts)

# Azure Data Factory (Conceptual - Not provided as code)
# - ADF Pipelines would orchestrate the execution of the above Databricks notebooks in sequence.
```

This structure follows a medallion architecture, processing data from raw (Bronze) to cleaned (Silver) and finally to aggregated, business-ready tables (Gold).


### ⚙️ Setup and Execution

---

This project is designed to be executed within an Azure Databricks environment, orchestrated by Azure Data Factory (ADF). Docker is **not** used for this project.

#### Prerequisites

1.  **Azure Subscription:** An active Azure subscription with access to Azure Data Lake Storage (ADLS) Gen2, Azure Databricks, and Azure Data Factory.
2.  **Azure Data Lake Storage (ADLS) Gen2:**
    *   A storage account with a container named `landingzone` (or as configured in `Bronze_Config.ipynb`) where the initial raw parquet files (e.g., `SalesLT_Address.parquet`, `SalesLT_Customer.parquet`, etc.) are located.
    *   Appropriate permissions for the Databricks service principal or cluster to read from the landing zone and write to the Bronze, Silver, and Gold layer storage locations (typically separate containers or directories within ADLS Gen2, e.g., `adlslmcompany_bronze`, `adlslmcompany_silver`, `adlslmcompany_gold`).
3.  **Azure Databricks Workspace:**
    *   A running Databricks cluster with the necessary libraries (PySpark is standard). The notebooks use standard PySpark and Delta Lake functionalities.
    *   The project notebooks uploaded to the Databricks workspace, maintaining the described folder structure (e.g., `/Workspace/Bronze/`, `/Workspace/Silver/`, etc.).
    *   The `Utils.ipynb` notebook should be accessible by other notebooks (e.g., placed in `/Workspace/Utils/` and referenced via `%run /Workspace/Utils/Utils`).
4.  **Azure Data Factory (for orchestration):**
    *   An ADF instance linked to your Azure Databricks workspace.
    *   ADF pipelines would be created to execute the Databricks notebooks in the correct sequence (Bronze -> Silver -> Gold).

#### ⚙️ Environment Configuration (Databricks Notebooks)

1.  **Bronze_Config.ipynb:**
    *   Verify and update the `configs` dictionary within this notebook. This dictionary defines the `file_path` for each source table in the landing zone, the target `table_name`, `schema_name` (e.g., `managed_bronze`), and `input_format`.
    *   Ensure the `abfss://landingzone@adlslmcompany.dfs.core.windows.net/` paths correctly point to your ADLS Gen2 landing zone container and storage account.
2.  **Databricks Secrets (Recommended for Production):**
    *   For connecting to ADLS Gen2, it is recommended to use secrets stored in Azure Key Vault and accessed via Databricks secrets scopes, rather than hardcoding credentials or access keys.

#### 🚀 How to Run the Notebooks (Manual Execution in Databricks for Development/Testing)

While ADF is the intended orchestrator for production, the notebooks can be run manually in Databricks for development and testing.

1.  **Navigate to your Azure Databricks Workspace.**
2.  **Upload Notebooks:** Ensure all project notebooks are uploaded to your workspace, maintaining the directory structure (e.g., `Bronze/`, `Silver/`, `Gold_DataMart/`, `Utils/`).
3.  **Run Bronze Layer Notebooks:**
    *   First, ensure `Bronze_Config.ipynb` is correctly configured.
    *   Open and run the `Bronze_Ingestion.ipynb` notebook. This notebook typically takes `table_name` and `schema_name` as parameters (widgets). You would run this notebook for each table defined in `Bronze_Config.ipynb` by setting the widget values accordingly before each run, or an ADF pipeline would iterate through these configurations.
    *   *Note:* The `Bronze_Ingestion (Not in use, With Loop).ipynb` is an alternative and not part of the primary flow.
4.  **Run Silver Layer Notebooks:**
    *   After the Bronze layer tables are populated, run the Silver layer notebooks (e.g., `Silver_Saleslt_Address.ipynb`, `Silver_Saleslt_Customer.ipynb`, etc.) in any order, as they typically process individual tables independently.
    *   These notebooks read from the Bronze layer (e.g., `adlslmcompany_bronze.managed_bronze.saleslt_address`) and write to the Silver layer (e.g., `adlslmcompany_silver.managed_silver.saleslt_address`).
    *   Ensure the `%run /Workspace/Utils/Utils` command at the beginning of these notebooks correctly points to your `Utils.ipynb` location.
5.  **Run Gold Layer (Data Mart) Notebooks:**
    *   Once the Silver layer tables are processed, run the Gold layer notebooks (e.g., `dim_SalesLT_Address.ipynb`, `f_SalesLT_OrderHeader.ipynb`, etc.).
    *   These notebooks read from the Silver layer tables and create the final dimension and fact tables in the Gold layer (e.g., `adlslmcompany_gold.managed_gold.dim_address`).

#### ⚙️ Orchestration with Azure Data Factory (Conceptual)

In a production scenario:

1.  **Create Linked Service:** In ADF, create a Linked Service to your Azure Databricks workspace.
2.  **Create Datasets:** (Optional, but good practice) Create datasets in ADF pointing to your Delta tables in ADLS Gen2 for lineage and monitoring.
3.  **Create Pipelines:**
    *   **Bronze Pipeline:** Create an ADF pipeline with a ForEach activity that iterates through the table configurations (potentially sourced from a configuration file or parameters). Inside the ForEach, use a Databricks Notebook activity to call `Bronze_Ingestion.ipynb` with the appropriate `table_name` and `schema_name` parameters.
    *   **Silver Pipeline:** Create an ADF pipeline with multiple Databricks Notebook activities to run each of the Silver layer notebooks. These can often run in parallel if there are no interdependencies between them.
    *   **Gold Pipeline:** Create an ADF pipeline with Databricks Notebook activities to run the Gold layer notebooks, ensuring dependencies are met (e.g., dimension tables are created before fact tables that reference them).
    *   **Master Pipeline:** Create a master pipeline that executes the Bronze, Silver, and Gold pipelines in sequence.
4.  **Triggers:** Set up triggers (e.g., schedule-based, event-based) in ADF to automate the pipeline execution.

This project **does not include Docker configuration** as it is intended to run within the Azure managed services (Databricks and ADF).


### ⚠️ Considerations

---

-   **Azure Environment Dependency:** This project is tightly coupled with Azure services (Azure Databricks, Azure Data Factory, Azure Data Lake Storage Gen2). It is not designed to run outside of this ecosystem.
-   **No Docker Usage:** As specified, this project does not use Docker. Deployment and execution rely on Azure's managed services.
-   **Simulated Orchestration in Notebooks:** Several notebooks (especially in the Silver and Gold layers) include comments like: *"IMPORTANT: Please note that this is a simulated project; the upsert operation will be executed within this notebook. In a production environment, a dedicated notebook containing only the function and validations would be developed. All function notebooks would be orchestrated by Azure Data Factory (ADF) pipelines or Azure Databricks (ADB) workflows."* This means that while the core logic is present, the end-to-end orchestration via ADF is conceptual in the provided materials and would need to be built out in an ADF instance.
-   **Parameterization and Configuration:**
    -   The `Bronze_Config.ipynb` centralizes some configurations, but in a production ADF pipeline, parameters (like table names, schema names, paths) would typically be passed dynamically to Databricks notebooks.
    -   Consider using ADF global parameters or configuration files for better manageability across pipelines.
-   **Secrets Management:** For accessing ADLS Gen2 or other secured resources, always use Azure Key Vault integrated with Databricks secrets scopes to avoid hardcoding credentials.
-   **Error Handling and Logging:**
    -   The provided notebooks include basic print statements. Production pipelines require robust error handling, logging (e.g., to Azure Log Analytics), and alerting mechanisms within both Databricks notebooks (e.g., try-except blocks, custom logging functions) and ADF pipelines.
-   **Idempotency:** The `upsert_table` function in `Utils.ipynb` helps ensure idempotency at the table level for Silver and Gold layers. Ensure the overall pipeline design maintains idempotency, especially for the Bronze ingestion if re-runs are possible.
-   **Scalability and Performance:**
    -   Azure Databricks and Delta Lake are inherently scalable. Cluster configurations (node types, auto-scaling) should be optimized based on data volume and processing complexity.
    -   Review Spark UI for performance bottlenecks and optimize Spark SQL queries and DataFrame operations (e.g., partitioning, caching strategies).
-   **Data Quality and Validation:**
    -   The `_validate_schema` function in `Utils.ipynb` provides a basic schema check. Implement more comprehensive data quality checks (e.g., using libraries like Great Expectations, or custom Spark validation rules) at each layer of the medallion architecture.
-   **Cost Management:**
    -   Monitor Databricks cluster usage and optimize for cost (e.g., using appropriate cluster types, auto-termination, spot instances where applicable).
    -   Optimize ADLS storage costs (e.g., lifecycle management policies).
-   **Testing:** Implement a testing strategy, including unit tests for utility functions and transformation logic, and integration tests for pipeline stages.
-   **Modularity of Utility Functions:** The `Utils.ipynb` is a good start. For larger projects, consider packaging utility code into Python libraries that can be attached to Databricks clusters for better code management and reusability.


### 🔮 Future Implementations

---

-   **Full Azure Data Factory Orchestration:** Develop and deploy the complete set of ADF pipelines (Bronze, Silver, Gold, and a Master pipeline) with robust parameterization, error handling, and logging to fully automate the end-to-end data processing workflow.
-   **Advanced Data Quality Monitoring:** Integrate a dedicated data quality framework (e.g., Great Expectations) to define and enforce comprehensive data quality rules and alerts at each stage of the pipeline.
-   **Schema Evolution Management:** Enhance the handling of schema evolution in Delta tables, potentially using Delta Lake's schema evolution features more explicitly or integrating schema registry tools.
-   **CI/CD for Databricks Notebooks and ADF Pipelines:** Implement CI/CD (Continuous Integration/Continuous Deployment) pipelines using Azure DevOps or GitHub Actions to automate the testing and deployment of Databricks notebooks and ADF pipeline configurations.
-   **Enhanced Logging and Monitoring:** Integrate with Azure Monitor and Log Analytics for centralized logging, performance monitoring, and alerting across both Databricks jobs and ADF pipelines.
-   **Delta Live Tables (DLT):** Explore migrating parts of the pipeline, particularly the Bronze and Silver layers, to Delta Live Tables for a more declarative and managed ETL experience within Databricks.
-   **Data Governance and Lineage:** Implement data governance tools and practices, and leverage Azure Purview for enhanced data discovery, classification, and end-to-end lineage tracking.
-   **Incremental Data Processing for All Layers:** While upserts are used, ensure all layers are optimized for incremental processing where applicable, especially if source systems provide change data capture (CDC) feeds.
-   **Unit and Integration Testing Framework:** Develop a more formal testing framework for Databricks notebooks, including unit tests for transformation logic and integration tests for pipeline segments.
-   **Dynamic Configuration Management:** Move more configurations (e.g., table lists, business rules thresholds) out of notebooks and into external configuration files or ADF parameters for easier management without code changes.
-   **Security Enhancements:** Further harden security by implementing network security (e.g., VNet integration for Databricks and ADF), fine-grained access control on Delta tables, and regular security audits.


### 🔗 References

---

-   **Azure Databricks Documentation:** [https://docs.microsoft.com/en-us/azure/databricks/](https://docs.microsoft.com/en-us/azure/databricks/)
-   **Azure Data Factory Documentation:** [https://docs.microsoft.com/en-us/azure/data-factory/](https://docs.microsoft.com/en-us/azure/data-factory/)
-   **Delta Lake Documentation:** [https://docs.delta.io/latest/index.html](https://docs.delta.io/latest/index.html)
-   **Apache Spark PySpark API Reference:** [https://spark.apache.org/docs/latest/api/python/](https://spark.apache.org/docs/latest/api/python/)
-   **Medallion Architecture in Data Lakehouse:** [https://www.databricks.com/glossary/medallion-architecture](https://www.databricks.com/glossary/medallion-architecture)

