**spark-sql-etl-framework**
==============
Multi Stage SQL based ETL Processing Framework Written in PySpark:  

process_sql_statements.py is a PySpark application which reads config from a YAML document (see config.yml in this project).  The configuration specifies a set of input sources - 
which are table objects avaiable from the catalog of the current SparkSession (for instance an AWS Glue Catalog) - in the `sources` section.  The `transforms` section is a list of   
transformations written as SQL statements using temporary views in Spark SQL, this is akin to using CTE (common table expressions) or Volatile Tables when performing typical multi-stage 
complex ETL routines on traditional relation database systems.  The `targets` section defines the location to write out the final output object to.  

The sample configuration uses the framework (`process_sql_statements.py`) to process a multi stage SQL ETL routine using data from the [AWS Sample Tickit Database](https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html)  
which have been stored as S3 objects and catalogued using Hive/AWS Glue.  Modify the `config.yml` file to specify your targets, projections, filters and transformations and run as follows:   

    spark-submit process_sql_statements.py config.yml
    
Dependencies
--------------
- Spark 2.x