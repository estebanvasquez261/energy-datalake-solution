import boto3
import os
from datetime import datetime
from urllib.parse import unquote_plus
#from dotenv import load_dotenv

s3 = boto3.client('s3')
glue = boto3.client('glue') 

# if not os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
#     load_dotenv()

DEST_BUCKET = os.environ.get("DEST_BUCKET")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME") 

def lambda_handler(event, context):
    for record in event['Records']:
        src_bucket = record['s3']['bucket']['name']
        src_key = unquote_plus(record['s3']['object']['key'])

        if not src_key.endswith(".csv"):
            print(f"Ignorado: {src_key} (no es un archivo CSV)")
            continue

        load_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        file_name = os.path.basename(src_key)
        folder_name_parts = file_name.split('_', 1)
        file_type = folder_name_parts[0]
        
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]
        name_without_ext, file_extension = os.path.splitext(file_name)
        new_file_name = f"{name_without_ext}_{timestamp}{file_extension}"
        
        dest_key = f"raw/{file_type}/fecha_carga={load_date}/{new_file_name}"
        full_dest_path = f"s3://{DEST_BUCKET}/{dest_key}"

        try:
            print(f"Copiando {src_key} desde {src_bucket} hacia {dest_key} en {DEST_BUCKET}")
            s3.copy_object(
                Bucket=DEST_BUCKET,
                CopySource={'Bucket': src_bucket, 'Key': src_key},
                Key=dest_key
            )
            
            print(f"Eliminando {src_key} desde {src_bucket}")
            s3.delete_object(Bucket=src_bucket, Key=src_key)

            if GLUE_JOB_NAME:
                print(f"Iniciando Glue Job '{GLUE_JOB_NAME}'")
                response = glue.start_job_run(
                    JobName=GLUE_JOB_NAME,
                    Arguments={
                        '--FILE_TYPE': file_type,
                        '--FILE_PATH': full_dest_path,
                        '--DEST_BUCKET': DEST_BUCKET,
                        '--LOAD_DATE': load_date
                    }
                )
                print(f"Glue Job '{GLUE_JOB_NAME}' iniciado con RunId: {response['JobRunId']}")
            else:
                print("La variable de entorno GLUE_JOB_NAME no está configurada. No se inició ningún Glue Job.")

        except Exception as e:
            print(f"Error al copiar {src_key}: {str(e)}")
            raise e

    return {"status": "ok"}