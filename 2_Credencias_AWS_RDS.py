# Databricks notebook source
# MAGIC %md
# MAGIC #### Credenciales para enviar el dataframe hacia AWS RDS

# COMMAND ----------

"""Leer las credenciales el cual esta en un csv"""

aws_rds_df = spark.read.format("csv")\
                        .option("sep",";")\
                        .option("header","true")\
                        .option("inferSchema","true")\
                        .load("/FileStore/tables/Key/jlmm_accessKeys_AWS_RDS.csv")

"Guadrando las credenciales en variables"
HOST = aws_rds_df.select("host").take(1)[0]["host"]
PORT = aws_rds_df.select("port").take(1)[0]["port"]
DATABASE = aws_rds_df.select("database_name").take(1)[0]["database_name"]
USER = aws_rds_df.select("user").take(1)[0]["user"]
PASSWORD = aws_rds_df.select("password").take(1)[0]["password"]
table = "llamadas"


"""Codigicado secrect key"""
import urllib
ENCODE_PASSWORD = urllib.parse.quote(string=PASSWORD, safe="")

driver = "org.mariadb.jdbc.Driver"
url = f"jdbc:mysql://{HOST}:{PORT}/{DATABASE}"