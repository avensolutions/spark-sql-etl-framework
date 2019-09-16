#
#
# process_sql_statements.py
#
# Process config driven, mutli-stage SQL based ETL using Spark SQL
#
# Example usage:
# spark-submit process_sql_statements.py config.yml

import yaml, sys, datetime
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
spark.sparkContext.setLogLevel("ERROR")

overall_start = datetime.datetime.now()

for job in config["jobs"]:
	# Load Sources
	print("Creating source views...")
	for source in job["sources"]:
		if source.get("table") is not None:
			print("Creating view %s from table %s..." % (source["view"], source["table"]))
			df = spark.table(source["table"])
		else:
			print("Creating view %s from object %s..." % (source["view"], source["object"]))
			df = spark.read.parquet(source["object"])
		if source.get("columns") is not None:
			# columns listed, select given columns
			df = df.select(source["columns"])
		if source.get("filters") is not None:
			for filter in source["filters"]:
				df = df.filter(filter)
		df.createOrReplaceTempView(source["view"])

	# Perform Transforms
	print("Performing SQL Transformations...")
	for transform in job["transforms"]:
		spark.sql(transform["sql"])

	# Write out final object
	print("Writing out final object to %s..." % (job["targets"]["target_location"]))
	start = datetime.datetime.now()
	final_df = spark.table(job["targets"]["final_object"])
	final_df.write.parquet(job["targets"]["target_location"], mode="overwrite")
	finish = datetime.datetime.now()
	print("Finished writing out target object...")

	### Remove this before productionizing
	#print("Sample output:")
	#spark.read.parquet(config["targets"]["target_location"]).show()
	###

	print("Total number of output rows: %s (%s)" % (str(spark.read.parquet(job["targets"]["target_location"]).count()), (str(finish-start))))

overall_finish = datetime.datetime.now()

print("total time taken : %s" % (str(overall_finish-overall_start)))
spark.sparkContext.stop()
