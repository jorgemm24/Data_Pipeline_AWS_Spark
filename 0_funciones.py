# Databricks notebook source
# MAGIC %md
# MAGIC #### Funcion para validar si el directorio exsite

# COMMAND ----------

def folder_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as error:
        if 'java.io.FileNotFoundException' in str(error):
            return False  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funcion para obtener todos los files de un directorio y subdirectorio 

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)