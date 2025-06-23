import sys
import os
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp, sha2, concat_ws, when, to_date


# 1. Inicialización de GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FILE_TYPE', 'FILE_PATH', 'DEST_BUCKET', 'LOAD_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Obtención de parámetros y configuración
file_type_param = args['FILE_TYPE']
file_path_param = args['FILE_PATH']
dest_bucket = args['DEST_BUCKET'] 
load_date = args['LOAD_DATE']

print(f"Iniciando Glue Job para tipo de dato: {file_type_param}")
print(f"Procesando archivo S3: {file_path_param}")

# 3. Leer el archivo CSV de la zona RAW
try:
    source_df = spark.read.csv(
        file_path_param,
        header=True,       
        inferSchema=True,
        sep=",",           
        quote='"',         
        escape='\\'
    )
    print(f"Archivo {file_path_param} leído con éxito.")
    source_df.printSchema()
    source_df.show(5, truncate=False)

except Exception as e:
    print(f"Error crítico al leer el archivo CSV {file_path_param}: {str(e)}")
    job.commit()
    sys.exit(1) 

# 4. Aplicar transformaciones específicas según el tipo de archivo
processed_df = None
output_table_name = ""

if file_type_param == "proveedores":
    print("Aplicando transformaciones para datos de PROVEEDORES...")
    output_table_name = "dim_proveedores"
    # 1. Selección y estandarización de nombres de columnas.
    # 2. Limpieza de datos
    # 3. Añadir metadatos de auditoría
    processed_df = source_df.select(
        col("ID_Proveedor").cast("string").alias("supplier_id"),
        col("NombreProveedor").alias("supplier_name"),
        col("TipoEnergia").alias("energy_type"), 
        col("PaisOrigen").alias("country_of_origin"),
        col("FechaInicioContrato").cast("date").alias("contract_start_date")
    ).filter(
        col("supplier_id").isNotNull()
    ).withColumn(
        "load_timestamp_utc", current_timestamp()
    ).withColumn(
        "source_file", lit(os.path.basename(file_path_param))
    ).withColumn(
        "load_date_partition", to_date(lit(load_date))
    )

elif file_type_param == "clientes":
    print("Aplicando transformaciones para datos de CLIENTES...")
    output_table_name = "dim_clientes"
    # 1. Selección y estandarización de nombres de columnas.
    # 2. Derivación de nueva columna
    # 3. Enmascarar/anonimizar PII básica si aplica
    processed_df = source_df.select(
        col("ID_Cliente").cast("string").alias("customer_id"),
        col("NombreCliente").alias("customer_name"),
        col("TipoCliente").alias("customer_type_raw"),
        col("Direccion").alias("address"),
        col("Ciudad").alias("city"),
        col("Pais").alias("country")
    ).withColumn(
        "customer_category", 
        when(col("customer_type_raw").like("%Residencial%"), "Residential")
        .when(col("customer_type_raw").like("%Comercial%"), "Commercial")
        .when(col("customer_type_raw").like("%Industrial%"), "Industrial")
        .otherwise("Other")
    ).withColumn(
        "hashed_customer_id", sha2(col("customer_id"), 256)
    ).withColumn(
        "load_timestamp_utc", current_timestamp()
    ).withColumn(
        "source_file", lit(os.path.basename(file_path_param))
    ).withColumn(
        "load_date_partition", to_date(lit(load_date))
    )

elif file_type_param == "transacciones":
    print("Aplicando transformaciones para datos de TRANSACCIONES...")
    output_table_name = "fact_transacciones_energia"
    # Transformaciones básicas para Transacciones:
    # 1. Renombrar y castear tipos de datos.
    # 2. Calcular nuevas métricas (ej. costo total).
    # 3. Particionar por fecha de transacción para optimizar consultas.
    processed_df = source_df.select(
        col("ID_Transaccion").cast("string").alias("transaction_id"),
        col("ID_Cliente").cast("string").alias("customer_id"),
        col("ID_Proveedor").cast("string").alias("supplier_id"),
        col("FechaTransaccion").cast("timestamp").alias("transaction_timestamp"),
        col("CantidadEnergiaMWh").cast("double").alias("energy_quantity_mwh"),
        col("PrecioPorMWh").cast("double").alias("price_per_mwh")
    ).withColumn(
        "total_cost", col("energy_quantity_mwh") * col("price_per_mwh")
    ).withColumn(
        "load_timestamp_utc", current_timestamp()
    ).withColumn(
        "source_file", lit(os.path.basename(file_path_param))
    ).withColumn(
        "transaction_date", col("transaction_timestamp").cast("date")
    ).withColumn(
        "load_date_partition", to_date(lit(load_date))
    )


else:
    print(f"ADVERTENCIA: No se encontró lógica de transformación específica para el tipo de dato '{file_type_param}'.")
    processed_df = source_df.withColumn(
        "load_timestamp_utc", current_timestamp()
    ).withColumn(
        "source_file", lit(os.path.basename(file_path_param))
    ).withColumn(
        "data_category", lit(file_type_param)
    )
    output_table_name = f"unrecognized_data_{file_type_param}"


if processed_df is None:
    print("ERROR: El DataFrame procesado es None. Verifique la lógica de transformación.")
    job.commit()
    sys.exit(1)

print(f"Esquema del DataFrame procesado para '{file_type_param}':")
processed_df.printSchema()
print(f"Primeras 5 filas del DataFrame procesado para '{file_type_param}':")
processed_df.show(5, truncate=False)


output_path = f"s3://{dest_bucket}/processed/{output_table_name}/"

processed_df.write.mode("append").partitionBy("load_date_partition").parquet(output_path)

print("Datos escritos con éxito a la zona procesada.")

job.commit()
print("Glue Job completado exitosamente.")