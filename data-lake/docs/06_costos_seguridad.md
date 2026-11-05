# 6. Costos y seguridad

## Estimación de costos

Precios de us-east-1, on-demand. Supuestos: unas 15 horas de EC2 encendida durante el laboratorio, 3 días de disco, alrededor de 100 noticias analizadas y unas 200 consultas de Athena.

| Servicio | Base de cálculo | Costo estimado |
|---|---|---|
| EC2 m7i-flex.large | ~0,096 USD/h × 15 h | ~1,45 USD |
| EBS gp3 30 GB | ~0,08 USD/GB-mes × 30 GB × 3 días | ~0,25 USD |
| Comprehend | 0,0001 USD por unidad de 100 caracteres, mínimo 3 unidades por solicitud. 3 APIs por noticia (idioma, entidades, frases) = 9 unidades | ~0,0009 USD por noticia, ~0,10 USD por 100 noticias |
| Athena | 5 USD por TB escaneado, mínimo 10 MB por consulta | ~0,01 USD |
| S3 | Pocos MB de almacenamiento y unos miles de solicitudes | < 0,05 USD |
| Glue Data Catalog | Primer millón de objetos y solicitudes sin costo | 0 USD |
| **Total del laboratorio** | | **~2 USD** |

Costo mensual si el pipeline quedara en operación continua: la EC2 encendida todo el mes (~70 USD) es el componente principal. Comprehend, con 12 ejecuciones diarias y unas 20 noticias nuevas al día, costaría menos de 1 USD al mes gracias a la caché. Para reducir el costo de la EC2 se podría usar una instancia programada o un servicio administrado de Airflow con encendido bajo demanda.

Controles de costo aplicados:
- Alerta de AWS Budgets en 10 USD mensuales.
- Caché por hash del texto. Comprehend no se vuelve a llamar para textos ya analizados, tampoco al reconstruir desde Bronze.
- Particiones por fecha para reducir los bytes escaneados por Athena.
- La instancia se detiene cuando no se usa y se elimina al terminar.

## Controles de seguridad

| Control | Implementación |
|---|---|
| Sin secretos en el código | La EC2 usa un rol IAM. No hay access keys en archivos, variables ni en Airflow. `.env` y `*.pem` están en `.gitignore` |
| Mínimo privilegio | La política permite S3 solo sobre el bucket del proyecto y únicamente las acciones necesarias de Comprehend, Glue y Athena |
| Bucket privado | Block Public Access activado y cifrado SSE-S3 |
| Acceso a la EC2 | Security group con solo SSH desde la IP del desarrollador. Autenticación por llave |
| Interfaz de Airflow sin exposición | El puerto 8080 no está abierto. Se accede por túnel SSH |
| Metadatos de la instancia | IMDSv2 obligatorio |
| Cifrado de conexiones en Airflow | `FERNET_KEY` generada al instalar |
| Parseo de XML | Se usa la librería estándar de Python, que limita la expansión de entidades. Los errores de parseo se registran como `malformed_xml` |
| Llamadas a fuentes externas | Timeout de 20 s y User-Agent identificable |

Limitaciones conocidas: el usuario `airflow/airflow` de la interfaz es el de la instalación por defecto. En un entorno compartido debería cambiarse. El docker-compose oficial de Airflow está pensado para desarrollo y no para producción.
