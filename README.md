AdventureWorks Lakehouse Project
This repository contains a comprehensive Lakehouse project utilizing the AdventureWorks database, built on Azure and Databricks. The project features data transformations across the gold, silver, and bronze layers, as well as the landing zone, following the medallion architecture pattern.

Project Overview
The primary objective of this project is to prepare the gold layer for dashboarding purposes, providing clean and structured data for analytical insights. The silver layer serves as the reliable source for all future gold layers, ensuring data consistency and trustworthiness. The bronze layer acts as the raw data storage, capturing data in its original form from various sources.

Key Features
Data Cleaning: Systematically removes unnecessary columns and ensures data types are consistent across different datasets.
Timestamp Addition: Adds processed timestamps to track when the data was processed, ensuring data lineage and auditability.
Self-Join on Product Category: Performs a self-join on the productcategory table to replace ParentProductCategoryID with the corresponding category name, enhancing data readability.
Data Enrichment: Joins various datasets to add meaningful information, improving the quality and usability of the data.
Modular Functions: Includes modular functions to handle specific data transformation tasks, promoting code reusability and maintainability.
Azure Integration: Configures and utilizes Azure services for data storage, processing, and management, ensuring scalability and reliability.
Azure Data Lake Storage Gen2: Utilizes Azure Data Lake Storage Gen2 for efficient and secure data storage, enabling separation of storage and compute costs, and taking advantage of fine-grained access control provided by Unity Catalog
1
2
.
Unity Catalog: Utilizes Unity Catalog for data governance and secure data sharing across the organization
2
.
Implementation Details
It is crucial to emphasize that these notebooks should ultimately be refined into final notebooks containing only the essential code. These notebooks should be orchestrated by Azure Data Factory (ADF) pipelines or Azure Databricks (ADB) workflows. This ensures that the ETL processes are automated, scalable, and maintainable.

Considerations
It is important to acknowledge that loading methods, error handling, logging mechanisms, and data validation checks were not implemented in this scenario, as these processes may vary based on business limitations, goals, data characteristics, and organizational choices. This example does not represent a real project, and the implementation details would depend on specific project requirements. The focus remains on demonstrating the potential structure and considerations for a project of this nature.

Additional Resources
A related project featuring the dashboard built on the gold layer is available in another repository. This repository focuses on the data transformation and enrichment processes, while the other repository provides insights into the dashboarding and visualization aspects.
