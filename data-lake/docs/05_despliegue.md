# 5. Despliegue, ejecución y eliminación

Región: us-east-1 (N. Virginia).

## Requisitos

- Cuenta de AWS en plan pagado (Paid plan). Con el plan gratuito, Comprehend devuelve `SubscriptionRequiredException`.
- Cliente SSH. En Windows sirve WSL.

## 1. Bucket S3

1. S3 → Create bucket → nombre único, por ejemplo `datalake-noticias-<usuario>-2026`.
2. Dejar activado "Block all public access" y el cifrado SSE-S3 por defecto.

## 2. Política y rol IAM

1. En `infra/iam_policy.json`, reemplazar `CAMBIAR-datalake-noticias` por el nombre del bucket (2 veces).
2. IAM → Policies → Create policy → JSON → pegar el archivo → nombre `datalake-noticias-policy`.
3. IAM → Roles → Create role → AWS service → EC2 → adjuntar la política → nombre `datalake-ec2-role`.

## 3. Instancia EC2

| Parámetro | Valor |
|---|---|
| AMI | Ubuntu Server 24.04 LTS, x86 |
| Tipo | m7i-flex.large (2 vCPU, 8 GB) |
| Key pair | RSA, .pem |
| Security group | Solo SSH (22) desde la IP propia |
| Disco | 30 GB gp3 |
| IAM instance profile | datalake-ec2-role |
| Metadata | IMDSv2 obligatorio, hop limit = 2 |

El hop limit 2 permite que los contenedores Docker obtengan las credenciales del rol.

## 4. Instalación de Airflow

Desde la máquina local:

```bash
chmod 400 ~/.ssh/<llave>.pem
ssh -i ~/.ssh/<llave>.pem -o ServerAliveInterval=30 -L 8080:localhost:8080 ubuntu@<IP_PUBLICA>
```

En la EC2:

```bash
git clone <url-del-repositorio> datalake-noticias
cd datalake-noticias
nano dags/config/pipeline.yaml        # escribir el nombre del bucket
bash infra/setup_ec2.sh               # instala Docker y termina
exit                                  # volver a entrar para aplicar el grupo docker
```

Volver a conectarse y ejecutar:

```bash
cd datalake-noticias
bash infra/setup_ec2.sh               # descarga Airflow 3.3.0, genera .env con FERNET_KEY y levanta los servicios
docker compose ps                     # esperar a que todos estén healthy
```

Airflow queda en `http://localhost:8080` a través del túnel SSH. El puerto 8080 no se abre a internet. Usuario y contraseña iniciales: `airflow` / `airflow`.

## 5. Verificación del entorno

En Airflow, ejecutar el DAG `smoke_test_aws`. Prueba la escritura en S3, una llamada a Comprehend, la instalación de pyarrow y la descarga de los 3 feeds.

## 6. Ejecución

DAG `noticias_datalake`:
- Programado cada 2 horas cuando está activado.
- Manual: Trigger. Parámetros:
  - `reprocess_run_id`: vacío para un lote nuevo, o el id de un lote de Bronze para reprocesarlo.
  - `rebuild_from_bronze`: reconstruye Silver con todos los lotes de Bronze.

Consultas de Athena (base `noticias_dl`):

```bash
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-worker python -m datalake.run_sql
```

También se pueden ejecutar los archivos de `dags/sql/` en la consola de Athena.

Herramienta de escenarios:

```bash
alias dl='docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-worker python -m datalake.scenarios'
dl estado                          # versión vigente, lotes y fallos activos
dl lote <run_id>                   # manifiestos, conteos, cuarentena y estado final de un lote
dl lote-defectuoso                 # crea un lote controlado con registros inválidos
dl lote-modificado                 # crea un lote controlado con una noticia modificada
dl fallo-activar silver_before_publish
dl fallo-quitar silver_before_publish
```

## 7. Actualizar el código

```bash
cd ~/datalake-noticias && git pull
```

Airflow detecta los cambios de `dags/` en uno o dos minutos. No hace falta reiniciar los contenedores.

## 8. Eliminación de recursos

En este orden:

1. En Airflow, desactivar el DAG `noticias_datalake`.
2. En la EC2: `cd ~/datalake-noticias && docker compose down --volumes --remove-orphans`.
3. EC2 → la instancia → Instance state → **Terminate** (no Stop, porque el disco sigue cobrando con la instancia detenida). Verificar que el volumen EBS se eliminó.
4. Glue → Databases → `noticias_dl` → Delete (elimina las 6 tablas y sus particiones).
5. S3 → el bucket → Empty → luego Delete.
6. IAM → eliminar el rol `datalake-ec2-role` y la política `datalake-noticias-policy`.
7. EC2 → Key pairs y Security groups → eliminar los creados para el proyecto.
8. Billing → revisar que no queden cargos activos. Mantener la alerta de Budgets.

Comprehend y Athena no tienen recursos que eliminar. Solo cobran por uso.
