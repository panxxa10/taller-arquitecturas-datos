# Ajustes por diferencias entre MongoDB y Amazon DocumentDB

Versión local: MongoDB 8.0 en Docker. Versión AWS: Amazon DocumentDB Serverless (motor 5.0.0, compatible con la API de MongoDB, no es MongoDB).

| # | Diferencia | Efecto en este proyecto | Ajuste realizado | Archivo |
|---|---|---|---|---|
| 1 | **TLS obligatorio** con el certificado de AWS (`global-bundle.pem`) | Sin TLS la conexión es rechazada | URI con `tls=true&tlsCAFile=...`; el certificado se descarga en la EC2 | `aws_config.py`, `scripts/aws/ec2_setup_sources.sh` |
| 2 | **No soporta retryable writes** | Los drivers modernos los activan por defecto y las escrituras fallan | `retryWrites=false` en el URI | `aws_config.py` |
| 3 | **Solo accesible dentro de la VPC** (sin endpoint público) | No se puede usar `localhost:27017` | Las extracciones corren en la EC2 (misma VPC); el security group de DocumentDB solo admite el de la EC2. Para Compass desde el portátil: túnel SSH (ver punto 3 del taller anterior) | `infra/terraform/main.tf` |
| 4 | **Topología de réplica `rs0`** y credenciales del secreto administrado | La contraseña la genera DocumentDB (usuario `docdbadmin`) y rota en Secrets Manager | `replicaSet=rs0&readPreference=secondaryPreferred`; usuario y contraseña se leen del secreto en ejecución, con `quote_plus` por si llevan caracteres especiales | `aws_config.py` |
| 5 | **Validación `$jsonSchema` soportada solo en parte** (`validationAction` únicamente `error`; en clústeres elásticos no existe) | `generate_data.py` crea las colecciones con validador estricto | Se intenta aplicar el validador; si DocumentDB lo rechaza se avisa y la colección se crea sin él. La calidad se garantiza igualmente aguas abajo con las capas `intermediate` y `quarantine` | `generate_data.py` |
| 6 | **`bypassDocumentValidation` soportado** en `insert` | Los registros defectuosos inyectados se insertan igual que en local | Sin cambios | `generate_data.py` |
| 7 | Compatibilidad parcial de operadores y comandos (no es MongoDB real) | Las extracciones solo usan `find`, `sort`, `insert_many`, `replace_one(upsert)` y `delete_many`, todos soportados | Sin cambios | `raw_ingestion.py` |

## Ajustes de la fuente relacional (RDS for MySQL 8.0)

- La base se llama `dw_source` (en local era `ecommerce`): variable `MYSQL_DATABASE`.
- El usuario es `admin` y su contraseña la administra RDS en Secrets Manager. Mejora recomendada fuera del taller: crear un usuario `etl` de solo lectura para las extracciones (mínimo privilegio).
- Conexión con TLS usando el mismo `global-bundle.pem`.
- Los datos son los mismos del proyecto local (`--seed 42`), así que los conteos esperados coinciden con las guías de `docs/etapas/`.
