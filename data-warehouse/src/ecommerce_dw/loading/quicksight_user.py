"""Crea (o rota la contraseña de) el usuario de solo lectura que usa QuickSight sobre el esquema marts.

  ./scripts/aws/ec2_run.sh ecommerce_dw.loading.quicksight_user
La contraseña se genera al azar y se muestra UNA sola vez en la terminal: se escribe en la consola de QuickSight y no se guarda
en el repositorio ni en archivos. El usuario solo puede leer el esquema marts (también las tablas que dbt cree después).
"""

import secrets

from ecommerce_dw.loading.redshift_data_api import DataApi

USER = "quicksight_ro"


def new_password():
    # Redshift exige 8-64 caracteres con mayúscula, minúscula y número; sin comillas, barras ni espacios.
    return "Qs1" + secrets.token_hex(12)


def statements(exists, password, owner):
    action = "ALTER USER" if exists else "CREATE USER"
    return [
        f"{action} {USER} PASSWORD '{password}'",
        f"GRANT USAGE ON SCHEMA marts TO {USER}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA marts TO {USER}",
        f'ALTER DEFAULT PRIVILEGES FOR USER "{owner}" IN SCHEMA marts GRANT SELECT ON TABLES TO {USER}',
    ]


def main(api=None):
    api = api or DataApi()
    owner = api.execute("SELECT current_user")[0][0]
    exists = bool(api.execute(f"SELECT 1 FROM pg_user WHERE usename = '{USER}'"))
    password = new_password()
    for sql in statements(exists, password, owner):
        api.execute(sql)
    print(f"Usuario de solo lectura {'actualizado' if exists else 'creado'}: {USER}")
    print(f"Contraseña (se muestra una sola vez; no la pegues en capturas ni la guardes en el repositorio): {password}")
    return password


if __name__ == "__main__":
    main()
