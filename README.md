## Pipeline y BI

La Empresa AFTO necesita analizar las atenciones de las llamadas de sus clientes.

Resumen del Proyecto:

Este proyecto consiste en realizar un pipeline y un dasbhoard de la información que envia la empresa AFTO, los archivos se reciben diariamente a las 23:00. El procesos inicia una vez depositado los archivos en el Datalake en la zona S3 se lanza el pipeline.

## Arquitectura
[![Arquitectura.png](https://i.postimg.cc/VkPN0KBc/Arquitectura.png)](https://postimg.cc/HV3mf4hS)

## Herramientas utilizadas

    - Databricks Comunnity: Spark, Pyspark
    - AWS capa gratuita: S3, Athena, RDS MySQL, IAM, VPC
    - Power BI


![aws](https://img.shields.io/badge/-Amazon%20AWS-232F3E?logo=Amazon-AWS&logoColor=white&style=flat-square) ![s3](https://img.shields.io/badge/-Amazon%20S3-569A31?logo=amazon-s3&logoColor=white&style=flat-square) ![RDS](https://img.shields.io/badge/-Amazon%20RDS-527FFF?logo=Amazon-RDS&logoColor=white&style=flat-square) ![databrics](https://img.shields.io/badge/-Databricks-FF3621?logo=Databricks&logoColor=white&style=flat-square) ![spark](https://img.shields.io/badge/-Apache%20Spark-E25A1C?logo=apache-spark&logoColor=white&style=flat-square) ![mysql](https://img.shields.io/badge/-MySQL-4479A1?logo=MySQL&logoColor=white&style=flat-square) ![power-bi](https://img.shields.io/badge/-Power%20BI-F2C811?logo=power-bi&logoColor=white&style=flat-square)



## Paso 0:
- Se crean los recursos necesarios en AWS capa gratuita:
	- S3, AWS RDS Mysql, IAM, Boto3, Athena, VPC




## Paso1:

- Crear estructura de Datalake en S3: 1_Bronze, 2_Silver, 3_Golden, Stage

[![1-Bucket-S3.png](https://i.postimg.cc/Qxmg7vKG/1-Bucket-S3.png)](https://postimg.cc/WhDJLW7X)

## Paso 2:
- Usar Boto3 para subir los archivos a la zona Stage
[![2-Bucket-S3-Stage.png](https://i.postimg.cc/xT88sZFS/2-Bucket-S3-Stage.png)](https://postimg.cc/8j8TcZSy)

## Paso 3:
- Usar databricks para realizar el procesamiento de la información en las tres zonas 1_Bronze, 2_Silver y 3_Golden.

- Los archivos .csv se mueven a la zona 1_Bronze y se organiza por directorios año-mes, si no existe los directiorios se crea automaticamente.

- Luego se leen los archivos de 1_bronze para realizar las transformaciones necesarias y se envia a la zona de paso Stage para luego moverlos a la zona 2_Silver. Se crean los directorios automaticamente y se guarda en formato .parquet

- Luego se lee los archivos de 2_Silver para realizar las agregaciones, se envia a la zona de paso Stage para luego moverlos a la zona de 3_Golden. Se crean los directorios automaticamente y se guarda en formato .parquet

[![7-Bucket-S3-Bronze-validando-folder-y-subfolder.png](https://i.postimg.cc/xCHMd1zY/7-Bucket-S3-Bronze-validando-folder-y-subfolder.png)](https://postimg.cc/VrfdG1gH)
[![7-1-Bucket-S3-Bronze-validando-folder-y-subfolder.png](https://i.postimg.cc/cCFHxpX0/7-1-Bucket-S3-Bronze-validando-folder-y-subfolder.png)](https://postimg.cc/n9QZTwyP)

[![8-Bucket-S3-de-Bronze-transformado-para-Stage-luego-moverlo-Hacia-Silver.png](https://i.postimg.cc/sxmcVgkx/8-Bucket-S3-de-Bronze-transformado-para-Stage-luego-moverlo-Hacia-Silver.png)](https://postimg.cc/N2yTDBfv)

- Zona 3_Golden con los archivos .parquet
[![10-3-Bucket-S3-archivos-golden-validando-archivos-del-mes.png](https://i.postimg.cc/3Rc8zXRx/10-3-Bucket-S3-archivos-golden-validando-archivos-del-mes.png)](https://postimg.cc/zbTYgRKs)

## Paso 4:

- Se envia la información del Datalake zona 3_Golden hacia AWS RDS MySQL

[![AWS-RDS-MYSQL.png](https://i.postimg.cc/L5kNWT9c/AWS-RDS-MYSQL.png)](https://postimg.cc/T5wVLnW0)

## Paso 5:

- Finalmente se crea un dashboard en Power BI

[![BI-1.png](https://i.postimg.cc/DfBWx792/BI-1.png)](https://postimg.cc/8sfP5gk3)

[![BI-2.png](https://i.postimg.cc/8cGqssjV/BI-2.png)](https://postimg.cc/ZBffMb8f)

[![BI-3.png](https://i.postimg.cc/zvk4h5gH/BI-3.png)](https://postimg.cc/tnYr0KD9)

[![BI-5.png](https://i.postimg.cc/4NJDgKD6/BI-5.png)](https://postimg.cc/0rBWGN2j)

[![BI-4.png](https://i.postimg.cc/pLS4zspC/BI-4.png)](https://postimg.cc/yDFLzXh3)


## Para una siguiente proyecto:
    - Secret Manager (administrar claves)
    - AWS Glue (para orquestar) u otro orquestador
    - AWS Lambda (para encender o apagar servicios)
    - Boto3 y EC2 para cargar archivos Stage

## Mail:
- ztejorge@hotmail.com
- Si deseas el archivo power bi, escribeme al mail.
