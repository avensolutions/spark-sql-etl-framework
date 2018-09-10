#
#
# process_sql_statements.py
#
# Process config driven, mutli-stage SQL based ETL using Spark SQL
#
# Example usage:
# spark-submit process_sql_statements.py config.yml

import yaml, sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

config_file  = sys.argv[1]

with open(config_file, 'r') as stream:
	config = yaml.load(stream)

print("Initializing SparkSession (%s)..." % (config["name"]))

spark = SparkSession \
	.builder \
	.appName(config["name"]) \
	.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load Sources
print("Creating source views...")
for source in config["sources"]:
	print("Creating view %s from table %s..." % (source["view"], source["table"]))
	df = spark.table(source["table"])
	if source.get("columns") is not None:
		# columns listed, select given columns
		df = df.select(source["columns"])
	if source.get("filters") is not None:
		for filter in source["filters"]:
			df = df.filter(filter)
	df.createOrReplaceTempView(source["view"])

# Perform Transforms
print("Performing SQL Transformations...")
for transform in config["transforms"]:
	spark.sql(transform["sql"])

# Write out final object
print("Writing out final object to %s..." % (config["targets"]["target_location"]))
final_df = spark.table(config["targets"]["final_object"])
final_df.write.parquet(config["targets"]["target_location"], mode="overwrite")
print("Finished writing out target object...")

### Remove this before productionizing 
print("Sample output:")
spark.read.parquet(config["targets"]["target_location"]).show()
###

print("Done!")