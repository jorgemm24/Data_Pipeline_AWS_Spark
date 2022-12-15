# Databricks notebook source
# MAGIC %md
# MAGIC #### Ejecutando modulos necesarios

# COMMAND ----------

# MAGIC %run ./0_funciones

# COMMAND ----------

# MAGIC %run ./1_Montado_AWS_S3

# COMMAND ----------

# MAGIC %run ./2_Credencias_AWS_RDS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Leer los archivos .csv de Stage hacia Bronze

# COMMAND ----------

""" 
Los archivos nuevos se guardan en la zona Stage para luego iniciar con el proceso el cual es
1. mover los archivos a la zona Bronce por Anio y Mes
2. leer los archivos de zona Bronce para realizar las transformaciones necesarias y enviarlas a la zona Silver
3. leer los archivos de la zona Silver y realizar las agregaciones necesarias y enviarlas a la zona Golden.
4. enviar los archivos de la zona Golden a un AWS RDS MySQL para su consumo

nota: se crea una zona stage el cual se usa para recibir los archivos nuevos y transformaciones para luego 
      moverlos a su respeciva zona (Bronce, Silver y Golden)
"""

# COMMAND ----------

import os

files_stage = dbutils.fs.ls("/mnt/mount_s3/Stage")
ruta_origen = "/mnt/mount_s3/Stage"
ruta_destino = "/mnt/mount_s3/1_Bronze"

# Crear directorio y mover los archivos dinamicamente por Anio/Mes de acuerdo al nombre del archivo
i=0
for file in files_stage:
    if file[1].endswith('.csv'):
        sepFile = file[1].split("_")
        anio = sepFile[2][:4]
        mes  = sepFile[2][4:6]
    
        # Si no existe el directorio lo crea
        if not folder_exists(f"{ruta_destino}/{anio}/{mes}"):
            dbutils.fs.mkdirs(f"{ruta_destino}/{anio}/{mes}")
        
        # Moviendo los files al directorio creado
        dbutils.fs.mv(f"{ruta_origen}/{file[1]}", f"{ruta_destino}/{anio}/{mes}/{file[1]}", True)       
        i +=1

print(f"Se movieron {i} archivos .csv")
   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Leer los archivos de Bronze, realizar las transformaciones y enviarlos a Silver

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, DateType 

# (E)
schema_defined = StructType([StructField("Nombre servicio",StringType(), True),
                            StructField("ID de llamada", StringType(), True),
                            StructField("Modo de llamada", StringType(), True),
                            StructField("Id Contacto", StringType(), True),
                            StructField("Base de datos", StringType(), True),
                            StructField("Callerid2", IntegerType(), True),
                            StructField("Día", DateType(), True),
                             StructField("Hora", TimestampType(), True),
                             StructField("Agente", StringType(), True),
                            StructField("Descripción del agente", StringType(), True),
                             StructField("Número de intentos", StringType(), True),
                             StructField("Tipo de llamada", StringType(), True),
                             StructField("Causa de colgado", StringType(), True),
                             StructField("Estado llamada", StringType(), True),
                             StructField("Fecha completa llamada", TimestampType(), True),
                             StructField("Fecha respuesta llamada", DateType(), True),
                             StructField("Hora respuesta llamada", TimestampType(), True),
                             StructField("Fecha completa respuesta última llamada", TimestampType(), True),
                             StructField("Fecha colgado llamada", DateType(), True),
                             StructField("Hora colgado llamada", TimestampType(), True),
                             StructField("Fecha completa colgado llamada", TimestampType(), True),
                             StructField("Tiempo en cola llamada", TimestampType(), True),
                             StructField("Tiempo total llamada", TimestampType(), True),
                             StructField("Argumentario", StringType(), True),
                             StructField("Resultado", StringType(), True),
                             StructField("MOTIVO", StringType(), True),
                             StructField("SUBMOTIVO", StringType(), True),
                             StructField("Fecha cola", DateType(), True),
                             StructField("Hora cola", TimestampType(), True),
                             StructField("Fecha de tono", DateType(), True),
                             StructField("Hora de tono", TimestampType(), True),
                             StructField("Duración de tono", TimestampType(), True),
                             StructField("Duración de conversación", TimestampType(), True),
                             StructField("ticketsid", StringType(), True),
                             StructField("Tipo de Emisor", StringType(), True),
                             StructField("Medio de Consulta", StringType(), True),
                             StructField("Tipo de Documento", StringType(), True),
                             StructField("Nro de Documento", StringType(), True),
                             StructField("Celular Afiliado", StringType(), True),
                             StructField("Operador", StringType(), True),
                             StructField("Tipo de Cliente", StringType(), True),
                             StructField("Correo electrónico", StringType(), True),
                             StructField("SamId", StringType(), True),
                             StructField("Observaciones", StringType(), True)
                            ])


"""
Se leera todo los archivos de todo los periodos, luego solo se leera el periodo actual
"""

df = spark.read.format("csv")\
                .schema(schema_defined)\
                .option("recursiveFileLookup","true")\
                .option("sep",";")\
                .option("header","true")\
                .load("/mnt/mount_s3/1_Bronze/")  


# (T)
""" Se realiza las transformaciones necesarias"""

from pyspark.sql.functions import when,col,date_format,substring,lit,concat

df_t = df.select(date_format("Día","yyyyMM").alias("idmes_1"),
                 
                 date_format("Día","yyyyMM").alias("idmes_2"),
                 
                 col("Día").alias("fecha"),
                 
                 concat( date_format("Hora","HH:"), when(date_format("Hora","mm").cast("int")<=29, "00").otherwise("30") ).alias("rango_hora"),
                 
                 when(date_format("Hora","HH").cast("int").between(6,23) , "OK").otherwise("NO_OK").alias("hora_valida"),
                 
                 when(col("Modo de llamada").contains("VIP"),"VIP")\
                     .when(col("Modo de llamada").contains("ATC"),"ATC")\
                     .otherwise("Otro").alias("campania"),
                 
                 when( (col("Modo de llamada").contains("Entrante")) & (col("Tipo de llamada")=="Entrante"), "Entrante")\
                     .otherwise("Saliente").alias("direccion"),
                 
                 col("Agente").alias("agente"),
                 
                 col("Estado llamada").alias("estado_llamada"),
                 
                 when(col("MOTIVO")=="-" , "Consulta").otherwise(col("MOTIVO")).alias("motivo"),
                 
                 when(col("SUBMOTIVO")=="-", "Otras Consultas").otherwise(col("SUBMOTIVO")).alias("submotivo"),
                 
                 col("Tipo de Emisor").alias("tipo_emisor"),
                 
                 substring("Tipo de Cliente",7, len(df.columns[40])).alias("tipo_cliente"),
                 
                 col("Operador").alias("operador"),
                 
                 when ( (col("Estado llamada").isin("Atendida","Completada")) & ( date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int") < 45 )  , "NS-Dentro")\
                  .when ( (col("Estado llamada").isin("Atendida","Completada")) & ( date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int") >= 45 )  , "NS-Fuera")\
                 .otherwise("No-aplica").alias("nivel_servicio"),
                 
                 when ( (col("Estado llamada").isin("Atendida","Completada")) & ( date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int") < 45 )  , 1).otherwise(0).alias("NS"),
                 
                 (date_format("Tiempo total llamada","HH").cast("int")*3600 +  date_format("Tiempo total llamada","mm").cast("int")*60 +  date_format("Tiempo total llamada","ss").cast("int")).alias("Wra_Aten"),
                 
                 when ( (col("Estado llamada").isin("Atendida","Completada")) & ( date_format("Tiempo total llamada","HH").cast("int")*3600 +  date_format("Tiempo total llamada","mm").cast("int")*60 +  date_format("Tiempo total llamada","ss").cast("int") < 251 )  , 1).otherwise(0).alias("Aten_TMO"),
                 
                 lit(1).alias("Ent"),
                 
                 when(  ( col("Agente").isNotNull() )  & (col("Estado llamada")=="Atendida") , 1 )\
                  .when(  ( col("Agente")!="" ) & (col("Estado llamada")=="Atendida") , 1 )\
                 .otherwise(0).alias("Aten"),
                 
                 when(  ( col("Agente").isNull() ) & (col("Estado llamada")=="Abandonada") , 1 )\
                  .when(  ( col("Agente")=="" ) & (col("Estado llamada")=="Abandonada") , 1 )\
                 .otherwise(0).alias("Aban"),
                 
                 (when( (date_format("Tiempo total llamada","HH").cast("int")*3600 +  date_format("Tiempo total llamada","mm").cast("int")*60 +  date_format("Tiempo total llamada","ss").cast("int")).isNull(), 0 )\
                     .otherwise((date_format("Tiempo total llamada","HH").cast("int")*3600 +  date_format("Tiempo total llamada","mm").cast("int")*60 +  date_format("Tiempo total llamada","ss").cast("int"))) -   \
                 when( (date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int")).isNull(), 0 )\
                     .otherwise((date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int")))).alias("TMO"), 
                 # falta hora_ACW,pero no aparece en el CRM el campo
                 
                 when( (date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int")).isNull(), 0 )\
                     .otherwise((date_format("Tiempo en cola llamada","HH").cast("int")*3600 +  date_format("Tiempo en cola llamada","mm").cast("int")*60 +  date_format("Tiempo en cola llamada","ss").cast("int"))).alias("TME")

                
    )


# COMMAND ----------

""" 
Se realiza la particion por periodo y se envia a la zona de paso Stage
"""
df_t.repartition(3).write.format("parquet").partitionBy("idmes_1").mode("overwrite").save("/mnt/mount_s3/Stage/")

# COMMAND ----------

""" 
Mover los archivos transformados de Stage hacia Silver 
"""

files_stage = dbutils.fs.ls("/mnt/mount_s3/Stage")
ruta_origen = "/mnt/mount_s3/Stage"
ruta_destino_silver = "/mnt/mount_s3/2_Silver"


i=0        
# Creando Folder Dinamicamente en Silver <Anio/Mes>
for file in list(get_dir_content(ruta_origen)):
    if file.endswith('.parquet'):
        sepFile = file.split("/")
        anio = sepFile[4][8:12]
        mes  = sepFile[4][-2:]
        fileNamePart = sepFile[5][:10]
        
        # Si no existe el directorio lo crea
        if not folder_exists(f"{ruta_destino_silver}/{anio}/{mes}"):
            dbutils.fs.mkdirs(f"{ruta_destino_silver}/{anio}/{mes}")
        
        # Moviendo los files al directorio creado
        if f"{file}" != f"dbfs:{ruta_destino_silver}/{anio}/{mes}/{fileNamePart}_Reporte_llamadas_{anio}{mes}.parquet":
            dbutils.fs.mv(f"{file}", f"dbfs:{ruta_destino_silver}/{anio}/{mes}/{fileNamePart}_Reporte_llamadas_{anio}{mes}.parquet")
        
            i += 1
            
print(f"Archivos procesados: {i}")

""" 
Limpiar la zona stage una vez cargados a Silver
"""
for file in files_stage:
    if file.isDir() or file.isFile():
        dbutils.fs.rm(file.path, recurse=True)
        

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Leer los archivos de Silver, se realiza las agregaciones y se envia a Golden

# COMMAND ----------

# (E)

df_silver = spark.read.format("parquet")\
                .option("recursiveFileLookup","true")\
                .load("/mnt/mount_s3/2_Silver/")   


# Agrupando (T)  
from pyspark.sql.functions import sum

df_tfinal = df_silver.groupBy("idmes_2","fecha","rango_hora","hora_valida","campania","direccion","agente","estado_llamada","motivo","submotivo","tipo_emisor","tipo_cliente","operador","nivel_servicio")\
                        .agg(sum("NS").alias("NS"), 
                             sum("Wra_Aten").alias("Wra_Aten"),
                             sum("Aten_TMO").alias("Aten_TMO"),
                             sum("Ent").alias("Ent"),
                             sum("Aten").alias("Aten"),
                             sum("Aban").alias("Aban"),
                             sum("TMO").alias("TMO"),
                             sum("TME").alias("TME")
                            )




# COMMAND ----------

""" 
Se realiza la particion por periodo y se envia a la zona de paso Stage
"""
df_tfinal.repartition(2).write.format("parquet").partitionBy("idmes_2").mode("overwrite").save("/mnt/mount_s3/Stage/")

# COMMAND ----------

""" 
Mover los archivos transformados de Stage hacia Golden 
"""

files_stage = dbutils.fs.ls("/mnt/mount_s3/Stage")
ruta_origen = "/mnt/mount_s3/Stage"
ruta_destino_golden = "/mnt/mount_s3/3_Golden"


i=0        
# Creando Folder Dinamicamente en Silver <Anio/Mes>
for file in list(get_dir_content(ruta_origen)):
    if file.endswith('.parquet'):
        sepFile = file.split("/")
        anio = sepFile[4][8:12]
        mes  = sepFile[4][-2:]
        fileNamePart = sepFile[5][:10]
        
        # Si no existe el directorio lo crea
        if not folder_exists(f"{ruta_destino_golden}/{anio}/{mes}"):
            dbutils.fs.mkdirs(f"{ruta_destino_golden}/{anio}/{mes}")
        
        # Moviendo los files al directorio creado
        if f"{file}" != f"dbfs:{ruta_destino_golden}/{anio}/{mes}/{fileNamePart}_Reporte_llamadas_{anio}{mes}.parquet":
            dbutils.fs.mv(f"{file}", f"dbfs:{ruta_destino_golden}/{anio}/{mes}/{fileNamePart}_Reporte_llamadas_{anio}{mes}.parquet")
        
            i += 1
            
print(f"Archivos procesados: {i}")


""" 
Limpiar la zona stage una vez cargados a Golden
"""

for file in files_stage:
    if file.isDir() or file.isFile():
        dbutils.fs.rm(file.path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingesta de archivos de la zona Golden hacia AWS RDS MySQL

# COMMAND ----------

""" dataframe Golden"""

df_golden = spark.read.format("parquet")\
                .option("recursiveFileLookup","true")\
                .load("/mnt/mount_s3/3_Golden/")  

# print(df_golden.count())  



""" Escribir el dataframe df_golden hacia AWS RDS MySQL"""

(df_golden.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable", table)
  .option("user", USER)
  .option("password", ENCODE_PASSWORD)
  .mode("append")
  .save()
)
