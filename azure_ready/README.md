# Azure Deployment

This folder contains required code and instructions for the automated cloud data pipeline using Azure Data Factory and Azure Databricks.

### Prerequisites

- Azure Account

### Instructions

1. Create a Resource group for the pipeline.

2. Create a Storage account and three containers within it named :`bronze_layer`, `silver_layer` and `gold_layer` for the raw, ingested and processed data to be stored.

3. Set up Azure Data Factory. Go to **Manage** -> **Linked Service** and create two linked services : One for the API using REST and one for dumping the raw data to storage using Azure Data Lake Storage.

- For the REST Service : Give the base url : `https://api.nasa.gov/neo/rest/v1/`
- For the ADLS Service, connect it to your created storage account.

4. Go to **Author** -> **Datasets** and create two Datasets for the two Linked services you just set up.

- For the REST Service linked Dataset Format the relative url to include your NASA NEO API key (you can make use of parameters added in the REST linked service) and the date range. Example :

```
@concat('feed?start_date=', formatDateTime(adddays(utcnow(), -8), 'yyyy-MM-dd'), '&end_date=', formatDateTime(adddays(utcnow(), -1), 'yyyy-MM-dd'), '&api_key=', dataset().api_key)
```

- For the ADLS linked Dataset : Set up a file path where the raw data is to be dumped.

```
bronze-layer/neo-data.json
```

5. Create a pipeline and then drag the `Copy data` block within `Move and Transfrom`. Set up the Source to be the REST Dataset and Sink to be the ADLS Dataset. Trigger the pipeline to test if the API works.

6. Go to Azure Databricks, set up a Workspace and a Personal Compute (Cluster) and a notebooks. This notebook will do the data ingestion and processing. The code for it is available in `Databricks_notebook.py`.

7. Go back to Azure Data Factory -> Pipeline -> Databricks and drag the `Notebook` block to the canvas. Connect the success point of the `Copy data` block to this `Notebook` block. Create a Databricks linked service and connect the notebook you created to this block (using the Notebook path).

8. Add a trigger to run this pipeline daily.

### To connect to PowerBI

1. Go to Azure Databricks -> SQL Warehouses -> Serverless Starter Warehouse. Copy the Server hostname and HTTP path.

2. Within PowerBI -> Get Data -> Azure Databricks, input the hostname and HTTP path (also tick DirectQuery). You will be asked to authenticate. Authenticate via personal access token (This can be generated within the Connection details from the Serverless Starter Warehouse page).
