A group data engineering project designed to mimic a real world data engineering problem.


The scenario is that a retail company has an SQL database with raw data in it but they need it to be transformed and uploaded to a data warehouse in the desired format for company use.

The source data DB diagram can be viewed here: https://dbdiagram.io/d/637b4c6dc9abfc6111741e65
The DB diagram for the Data Warehouse can be viewed here: https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca

The source data will be updated regularly and any changes to the source database must be reflected in the data warehouse within 30 minutes. This was acheived using AWS Lambdas. Eventbridge was used to trigger the ingest data lambda every 5 minutes, the lambdas for processing and then uploading to the data warehouse were then triggered by put operations to relevent bucket.

Upon initial deployement three s3 buckets are created, an ingest bucket, a processed bucket and a code bucket. When triggered by AWS Eventbridge, the ingesting lambda runs a series of SQL queries to the source database, saving each table from each of the three schemas to the ingest bucket in csv format. Additionally it runs a script to create a 'run number file' which increments by 1 on each triggering of the Lambda and then incorporates that number into the file key to ensure only the most recent ingest is used by the processing functions.

The processing functions are then triggered upon put operations to the ingest bucket, they first find the latest run number then access the data from each file key from their schema with the corresponding run number. This data is transformed into Pandas dataframes then processed using pandas into the desired format for the data warehouse. Upon successful completion of the data processing, the resuolting dataframes are then saved into the processed data bucket in Parquet format. As with the ingested bucket the file keys were named with the name of the source schema, the name of the table and the run number.

Each successful run logged a message saying run successful using AWS Cloudwatch, any errors were also logged so that email alerts could be sent to warn of potential issues with the code.

