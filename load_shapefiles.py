# Databricks notebook source
# MAGIC %md
# MAGIC # Load shapefiles in Databricks with Sedona

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster setup
# MAGIC 
# MAGIC Open your cluster configuration and:
# MAGIC 
# MAGIC 1) From the `Libraries` tab install from Maven Coordinates
# MAGIC 
# MAGIC ```
# MAGIC org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating
# MAGIC ```
# MAGIC ```
# MAGIC org.datasyslab:geotools-wrapper:geotools-24.1
# MAGIC ```
# MAGIC 
# MAGIC 2) From the `Libraries` tab install from PyPI
# MAGIC 
# MAGIC ```
# MAGIC apache-sedona
# MAGIC ```
# MAGIC 
# MAGIC 3) (Optional; only for DBR <= 7.3 LTS) Speed up the serialization/deserialization by registering the KryoSerializer classes.
# MAGIC 
# MAGIC *This option is not compatible with newer DBR versions (8.x+).*
# MAGIC 
# MAGIC Go to your `Cluster` -> `Edit` -> `Configuration` -> `Advanced options` and add the following lines to the Spark Config:
# MAGIC 
# MAGIC ```
# MAGIC spark.serializer org.apache.spark.serializer.KryoSerializer
# MAGIC spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register geospatial SQL expressions
# MAGIC 
# MAGIC This will make the geospatial `ST_*` functions available to SQL and python expressions.
# MAGIC 
# MAGIC You can find the available `ST_*` [constructors](https://sedona.apache.org/api/sql/Constructor/), [functions](https://sedona.apache.org/api/sql/Function/), etc. on the Sedona documentation pages.

# COMMAND ----------

from sedona.register.geo_registrator import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download example data
# MAGIC 
# MAGIC We are using the NYC buildings dataset to demostrate the ingestion process. We will use python requests to ge the data.
# MAGIC 
# MAGIC The dataset is available on https://data.cityofnewyork.us/Housing-Development/Shapefiles-and-base-map/2k7f-6s2k

# COMMAND ----------

import requests
from pathlib import Path

# Define where to store the shape files
base_path = '/dbfs/tmp/geospatial'

nyc_buildings_zip = f'{base_path}/nyc_buildings.zip'
nyc_buildings_files = f'{base_path}/nyc_buildings'

base_dbfs_path = base_path.replace('/dbfs/', 'dbfs:/')
nyc_buildings_dbfs_path = nyc_buildings_files.replace('/dbfs/', 'dbfs:/')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Download the NYC buildings shapefiles

# COMMAND ----------

# Create directory if it does not exist
path = Path(base_path)
path.mkdir(parents=True, exist_ok=True)

# Download shapefiles
url = 'https://data.cityofnewyork.us/api/geospatial/2k7f-6s2k?method=export&format=Shapefile'

with open(nyc_buildings_zip, 'wb') as f:
  r = requests.get(url, allow_redirects=True)
  f.write(r.content)

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip the downloaded files

# COMMAND ----------

# Unzip the files
import zipfile

with zipfile.ZipFile(nyc_buildings_zip, 'r') as zip_ref:
    zip_ref.extractall(nyc_buildings_files)

# COMMAND ----------

display(dbutils.fs.ls(nyc_buildings_dbfs_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read shapefiles

# COMMAND ----------

from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter

nyc_buildings_rdd = ShapefileReader.readToGeometryRDD(inputPath=nyc_buildings_dbfs_path, sc=sc)
nyc_buildings = Adapter.toDf(nyc_buildings_rdd, spark)

# COMMAND ----------

# Display first 10 rows
display(nyc_buildings.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Count the number of records.
# MAGIC This will take a long time because shapefiles has to be entirely red sequentially by a single core.

# COMMAND ----------

nyc_buildings.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create the Bronze tables
# MAGIC 
# MAGIC By ingesting the geospatial data into a delta table, we can run much faster queries because the read can be paralelized and the delta table metadata can be leaveraged.

# COMMAND ----------

db = 'geospatial_example_bronze'
table_name = 'nyc_buildings'

spark.sql(f'CREATE DATABASE IF NOT EXISTS {db}')
spark.sql(f'USE {db}')

nyc_buildings.write.saveAsTable(f'{db}.{table_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Counting the rows from the delta table will be much faster compared to counting from shapefiles done above.

# COMMAND ----------

spark.read.table(table_name).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL queries
# MAGIC 
# MAGIC Now that the data is ingested into a bronze delta table, we can run SQL queries on top of it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_buildings

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT name, 
# MAGIC        round(Cast(num_floors AS DOUBLE), 0) AS num_floors,
# MAGIC        ST_Centroid(geometry) as centroid
# MAGIC FROM   nyc_buildings 
# MAGIC WHERE  name <> ''
# MAGIC ORDER  BY num_floors DESC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the nearest named buildings to a specific point 
# MAGIC SELECT name,
# MAGIC        ST_Distance(ST_Centroid(geometry), ST_GeomFromWKT('POINT (-74.01118252805256 40.712176182048815)')) as distance
# MAGIC FROM   nyc_buildings 
# MAGIC WHERE  name <> ''
# MAGIC ORDER BY distance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Remove the downloaded data
dbutils.fs.rm(base_dbfs_path, True)

# COMMAND ----------

# Drop the delta table
spark.sql(f'DROP TABLE IF EXISTS {table_name}')

# COMMAND ----------


