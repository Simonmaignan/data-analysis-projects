# Netquest follow-up questions

## 1. How would you choose to bundle (or not) the code for deployment to the Cloud (any Cloud Provider)?

### Docker

One common and versatile solution would be to deploy the code inside a container (e.g. `Docker`) which simplifies the dependencies management by having the script run in a consistent environment across different stages.

To do so, I'd need to create a `DockerFile` to bundle my Python script along with the necessary dependencies (like `pandas`).

The Docker solution is best if the script is part of a multi-scripts/package/services environment and allows it to scale up easily. It also allows you to easily switch between stages if needed.

### Serverless

If the script is meant to stay a standalone script, then having it run in a serverless architecture (e.g. AWS Lambda) is a better solution (lightweight and cost-efficient approach).

This could be the best solution to start with until scaling up is decided.

### Cloud data pipeline services

The script could also be deployed inside a cloud-native data pipeline service (e.g. AWS Glue).

This would provide some monitoring and scaling possibilities, however, this requires some overhead configuration and some costs that may not be worth it if the script stays standalone.

## 2. Instead of having around 5 rows per day, let’s imagine that the process has 20 million. What would you have done differently designing the transformation job?

### Adapt processing

I would have used a tool optimized for large data sets like `Apache Spark` (with its Python API `PySpark`), which is faster and more adapted to large-scale data sets than Pandas.

`Apache Spark` can handle data partitioning and distributed and parallel computation across multiple nodes and can thus better handle the memory and processing resources for large-scale data sets.

### Optimize Storage Format

CSVs are not an efficient storage format for big data. I would then consider moving the data set to a database like PostgreSQL or AWS Redshift.

## 3. The output data is currently stored in S3. The Data Analyst in the Finance team wants to have access to the data (they only know SQL). How would you provide the access? What if the data was 20 million rows per day?

### AWS RDS

The data could be moved to a [AWS RDS](https://aws.amazon.com/rds/) to simply set up a SQL engine (e.g. PostgreSQL or MySQL) and give access to the data via SQL data queries. Since RDS is a single-node engine, it's limited in data storage though.

### AWS Athena

If the data set is very large and since it's stored in an S3, we could then set up a [AWS Athena](https://aws.amazon.com/athena/) which is a serverless SQL-based query service that allows us to directly query data stored in an S3.

In both cases, to better handle the 20 million rows, we have to make sure that the data is partitioned (e.g. by date) to speed up the queries.

## 4. The old SoftwareA is still running in Production for some teams, so the mappings must run daily. How would you monitor the quality of the job results? How would you monitor changes to the SoftwareB CustomFiled values? (Changes to the CustomField would make our mapping excel file obsolete)

### Monitor data quality

I would set up some data validation and quality checks after each migration using:

* Data integrity checks: e.g. to compare counts between source and target data.
* Validation: Validate key fields against predefined rules (e.g. no `NaN`, values within expected ranges...)

Furthermore, a data quality dashboard (e.g. AWS Quicksight) could be set up to track key quality metrics.

## 5. We have a massive PostgreSQL cluster with millions of rows in some tables. Suddenly, we detect one slow query as the bottleneck of a pipeline. Please describe the actions you would do to diagnose and optimize the query

1. Identify and diagnose the problem.
    1. Using `EXPLAIN` SQL command shows how the query is being executed.
    2. Check system resources by monitoring PostgreSQL with the `pg_stat_activity` table to give insights into queries and their runtime.
2. Optimize the query
    1. Indexing is the most common way to speed up a slow query. We should ensure that columns used in `WHERE`, `JOIN`, `ORDER BY`, and `GROUP BY` are indexed.
    2. Rewrite the query
    3. Partitioning the large table can help speed up queries by limiting the amount of data that needs to be scanned (e.g. by date).
    4. Caching the frequently executed queries reduces their execution time.
3. Test and monitor the results to compare the performances and ensure that the optimization persists over time.

## 6. While working with files in a data lake, which are the main good practices that you would propose to make it robust and cost-effective?

1. Data organization and partitioning
    1. Use a clear folder structure.
    2. Partition large data set based on frequently queried fields.
2. File format optimization
    1. Compress files to reduce storage costs.
    2. Avoid small files since they can increase the access overhead.
3. Data Governance and Security
    1. Enforce data governance rules like metadata tagging and cataloging.
    2. Encrypt sensitive data.
    3. Set up role-based access control to manage who can access, modify, or delete data.
4. Data Quality
    1. Maintain schema consistency by using metadata catalogs.
    2. Automate data validation and check for quality issues (missing values, duplicate rows...).
5. Cost management
    1. Optimize storage cost by moving infrequently accessed data to low-performance (and cost) storage (eg. S3 Glacier).
    2. Manage file life cycle by defining rules to transition data between storage classes or delete it.
6. Monitoring
    1. Track usage and costs to understand how data is accessed and what it costs.
    2. Enable logging and auditing to ensure that all data access and modifications are logged.

## 7. In terms of working with other data teams, like Data Science, what would be your approach to making the collaboration successful regarding the interaction with them?

The first and most important point for a successful collaboration is the alignment and communication between the teams. I'd make sure that the teams share the same goals and understandings and I'd set up regular synchronous and asynchronous communication channels.

I'd make sure that the Data Engineering team ensures that the data scientists team has easy access to clean, structured, formatted, and versioned data.

I'd also make sure that the data provided is of high quality (cleaned and reliable) and documented.

I'd set up collaborative workflow and tools like version control and code sharing (e.g. Github) or a shared notebook environment (e.g. Jupyter Notebooks)

Finally, I'd set up some regular feedback loops between the 2 teams to help resolve issues and discuss areas for improvements.
