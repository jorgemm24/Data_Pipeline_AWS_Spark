# Databricks notebook source
# MAGIC %md
# MAGIC #### Leer credenciales AWS S3

# COMMAND ----------

"""
Las credenciales se exporto de aws en un csv y se lee desde databricks y se guardaran en 
las variables ACCESS_KEY, SECRET_KEY
"""

awskey_df = spark.read.format("csv")\
                        .option("sep",";")\
                        .option("header","true")\
                        .option("inferSchema","true")\
                        .load("/FileStore/tables/Key/jlmm_accessKeys_AWS.csv")

"Guadrando las credenciales de s3 en variables"
ACCESS_KEY = awskey_df.select("Access key ID").take(1)[0]["Access key ID"]
SECRET_KEY = awskey_df.select("Secret access key").take(1)[0]["Secret access key"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Codificando secrect key

# COMMAND ----------

import urllib

ENCODE_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Codificando secrect key

# COMMAND ----------

AWS_S3_BUCKET = "empresa-afto"
MOUNT_NAME = "/mnt/mount_s3"
SOURCE_URL = "s3a://%s:%s@%s" %(ACCESS_KEY,ENCODE_SECRET_KEY,AWS_S3_BUCKET)

dbutils.fs.mount(SOURCE_URL,MOUNT_NAME)