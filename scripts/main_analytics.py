import os
from athena_querier import AthenaQuerier
from dotenv import load_dotenv

load_dotenv()

def run_analytics_queries():
    """
    Función principal para ejecutar las consultas analíticas en Athena,
    utilizando un diccionario para gestionar las consultas.
    """
    # --- Configuración ---
    DEST_BUCKET = os.environ.get('DEST_BUCKET')
    ATHENA_DATABASE_NAME = os.environ.get('ATHENA_DATABASE_NAME')
    ATHENA_RESULTS_LOCATION = f"s3://{DEST_BUCKET}/athena_query_results/"
    AWS_REGION = os.environ.get('AWS_REGION')

    # Crea la instancia del AthenaQuerier
    querier = AthenaQuerier(
        s3_output_location=ATHENA_RESULTS_LOCATION,
        database_name=ATHENA_DATABASE_NAME,
        region_name=AWS_REGION
    )

    # Registro de Consultas
    queries_to_execute = {
        "numero_total_clientes": "athena_queries/count_clientes.sql",
        "top_proveedores": "athena_queries/top_proveedores.sql",
        "costo_transacciones_cliente": "athena_queries/total_cost_per_customer.sql"
    }

    #Ejecución de Consultas Usando el Diccionario
    for query_name, query_file_path in queries_to_execute.items():
        print(f"\n--- Ejecutando Consulta: {query_name} ---")
        try:
            df_result = querier.execute_query(
                query_source=query_file_path,
                is_file=True
            )
            print(f"Resultado de '{query_name}':")
            print(df_result)
        except Exception as e:
            print(f"ERROR: Falló la consulta '{query_name}': {e}")

if __name__ == "__main__":
    run_analytics_queries()