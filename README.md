# Proyecto de Integración de Datos con AWS

Este repositorio contiene la infraestructura y los scripts necesarios para cargar datos a Redshift desde archivos cargados en S3, utilizando servicios como AWS Lambda, AWS Glue y Amazon Athena.

---

## Estructura del Proyecto
```bash
.
architecture/           # Diagramas e infraestructura del sistema
athena_queries/         # Consultas SQL utilizadas por Athena
glue_jobs/              # Scripts de Glue Jobs (ETL)
lambda/                 # Código fuente de funciones Lambda
scripts/                # Consultas con Athena desde Python
.gitignore              # Archivos y carpetas ignoradas por Git
README.md               # Documentación del proyecto (este archivo)
```

---
## Objetivo

Automatizar el flujo de datos desde archivos cargados a un bucket de S3 hacia Redshift, siguiendo la siguiente arquitectura:

1. Una función Lambda se activa al detectar un archivo nuevo en S3.
2. Esta Lambda mueve el archivo a la capa raw y lanza un Glue Job.
3. El primer Glue Job lee los datos desde la capa raw, aplica transformaciones básicas y los escribe en la capa processed, particionando por fecha de carga.
4. Una Step Function se activa al finalizar exitosamente el primer Glue Job, y lanza un segundo Glue Job, el cual toma los datos desde la capa processed y los carga en Amazon Redshift.
5. Un Glue Crawler detecta el esquema de los archivos en la capa processed y lo registra automáticamente en el Data Catalog.
6. Adicionalmente, hay un script en Python que puede ejecutarse localmente para realizar consultas a los datos mediante Amazon Athena.

   ## Tecnologías Utilizadas

- **AWS Lambda**
- **AWS Glue**
- **Amazon Athena**
- **Amazon Redshift**
- **Amazon S3**
- **Python 3.12**
- **Boto3**
- **Pandas**
---
