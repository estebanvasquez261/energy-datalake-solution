import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# 1. Inicialización
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'LOAD_DATE',
    'FILE_TYPE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Parámetros
load_date = args['LOAD_DATE']
file_type = args['FILE_TYPE'].lower()

REDSHIFT_DATABASE = "dev"
REDSHIFT_CONNECTION_NAME = "Redshift connection"
REDSHIFT_TEMP_DIR = "s3://datalake-energy-temp-266032502077/redshift-temp/"
PROCESSED_S3_BUCKET = "datalake-energy-266032502077"

s3_base_path = f"s3://{PROCESSED_S3_BUCKET}/processed/"

table_key = f"fact_{file_type}" if file_type == "transacciones" else f"dim_{file_type}"

# 3. Configuración centralizada de tablas
redshift_tables_config = {
    "dim_proveedores": {
        "s3_path": f"{s3_base_path}dim_proveedores/load_date_partition={load_date}/",
        "redshift_table_name": "public.dim_proveedores",
        "upsert_keys": ["supplier_id"]
    },
    "dim_clientes": {
        "s3_path": f"{s3_base_path}dim_clientes/load_date_partition={load_date}/",
        "redshift_table_name": "public.dim_clientes",
        "upsert_keys": ["customer_id"]
    },
    "fact_transacciones": {
        "s3_path": f"{s3_base_path}fact_transacciones_energia/load_date_partition={load_date}/",
        "redshift_table_name": "public.fact_transacciones",
        "upsert_keys": ["transaction_id"]
    }
}

# 4. Función para upsert
def upsert_to_redshift(dynamic_frame, redshift_table_name, upsert_keys, connection_name, temp_dir):
    import uuid
    from awsglue.dynamicframe import DynamicFrame
    
    df = dynamic_frame.toDF()
    df_dedup = df.dropDuplicates(upsert_keys)
    
    dynamic_frame_clean = DynamicFrame.fromDF(df_dedup,
                                              glueContext,
                                              "dynamic_frame_clean")
    
    schema, base_table = redshift_table_name.split(".")
    staging_table_name = f"{schema}.{base_table}_staging_{uuid.uuid4().hex[:8]}"

    preactions = f"""
        DROP TABLE IF EXISTS {staging_table_name};
        CREATE TABLE {staging_table_name} (LIKE {redshift_table_name});
    """.replace('\n', ' ').strip()
    
    keys_match = " AND ".join([f"{redshift_table_name}.{k} = {staging_table_name}.{k}" for k in upsert_keys])
    
    postactions = f"""
        DELETE FROM {redshift_table_name}
        USING {staging_table_name}
        WHERE {keys_match};
        INSERT INTO {redshift_table_name}
        SELECT * FROM {staging_table_name};
        DROP TABLE {staging_table_name};
    """.replace('\n', ' ').strip()
    

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame_clean,
        catalog_connection=REDSHIFT_CONNECTION_NAME,
        connection_options={
            "database": REDSHIFT_DATABASE,
            "dbtable": staging_table_name,
            "preactions": preactions,
            "postactions": postactions
        },
        redshift_tmp_dir=REDSHIFT_TEMP_DIR,
        transformation_ctx=f"write_to_{redshift_table_name}"
    )

# 5. Procesamiento de la tabla solicitada
if table_key not in redshift_tables_config:
    raise ValueError(f"Tabla '{table_key}' no está definida en la configuración.")

config = redshift_tables_config[table_key]
s3_path = config["s3_path"]
redshift_table_name = config["redshift_table_name"]
upsert_keys = config["upsert_keys"]

print(f"Leyendo datos de {table_key} desde {s3_path}")

source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path], "recurse": True},
    format="parquet",
    transformation_ctx=f"read_{table_key}"
)

if source_df.count() > 0:
    print(f"Ejecutando upsert para {redshift_table_name}")
    upsert_to_redshift(source_df, redshift_table_name, upsert_keys, REDSHIFT_CONNECTION_NAME, REDSHIFT_TEMP_DIR)
else:
    print(f"No se encontraron datos para {table_key} en {s3_path}, omitiendo carga.")

# 6. Finalización
job.commit()
print("Job completado exitosamente.")
