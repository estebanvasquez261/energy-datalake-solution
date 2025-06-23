import boto3
import time
import pandas as pd
import os

class AthenaQuerier:
    def __init__(self, s3_output_location: str, database_name: str, region_name: str = 'us-east-1'):
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.s3_output_location = s3_output_location
        self.database_name = database_name
        self.region_name = region_name
        print(f"AthenaQuerier inicializado para DB: '{self.database_name}' en región: '{self.region_name}'")

    def _load_query_from_file(self, query_file_path: str) -> str:
        try:
            abs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', query_file_path)
            with open(abs_path, 'r') as file:
                query = file.read()
            return query
        except FileNotFoundError:
            raise FileNotFoundError(f"Archivo de consulta no encontrado: {query_file_path}")
        except Exception as e:
            raise Exception(f"Error al leer el archivo de consulta '{query_file_path}': {e}")

    def execute_query(self, query_source: str, is_file: bool = False, wait_for_completion: bool = True, poll_interval: int = 5) -> pd.DataFrame:
        query_string = query_source
        if is_file:
            query_string = self._load_query_from_file(query_source)
            print(f"Cargada consulta desde archivo: {query_source}")
        
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query_string,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            query_execution_id = response['QueryExecutionId']
            print(f"Consulta Athena iniciada con ID: {query_execution_id}")

            if wait_for_completion:
                return self._wait_for_query_completion(query_execution_id, poll_interval)
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Error al iniciar la consulta Athena: {e}")
            raise

    def _wait_for_query_completion(self, query_execution_id: str, poll_interval: int) -> pd.DataFrame:
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            print(f"Estado de la consulta {query_execution_id}: {state}")

            if state == 'SUCCEEDED':
                print(f"Consulta {query_execution_id} completada exitosamente. Obteniendo resultados...")
                return self._get_query_results(query_execution_id)
            elif state in ['FAILED', 'CANCELLED']:
                error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error.')
                print(f"La consulta {query_execution_id} falló o fue cancelada: {error_message}")
                raise Exception(f"Consulta Athena fallida/cancelada: {error_message}")
            else:
                time.sleep(poll_interval)

    def _get_query_results(self, query_execution_id: str) -> pd.DataFrame:
        try:
            paginator = self.athena_client.get_paginator('get_query_results')
            page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)

            all_rows = []
            column_names = []

            for page_number, page in enumerate(page_iterator):
                rows = page['ResultSet']['Rows']
                if page_number == 0:
                    column_info = page['ResultSet']['ResultSetMetadata']['ColumnInfo']
                    column_names = [col['Name'] for col in column_info]
                    rows = rows[1:]

                for row in rows:
                    values = [item.get('VarCharValue', None) for item in row['Data']]
                    all_rows.append(values)

            return pd.DataFrame(all_rows, columns=column_names)

        except Exception as e:
            print(f"Error al obtener o parsear resultados de la consulta {query_execution_id}: {e}")
            raise
