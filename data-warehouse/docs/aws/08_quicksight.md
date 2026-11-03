# Paso 8 – Dashboard en Amazon QuickSight

**Fuente:** `marts.rpt_sales_dashboard` (modelo dbt, una fila por línea de venta; su suma coincide con `marts.fct_sales`, con prueba dbt).
**Conexión privada:** Redshift no es accesible desde internet (`publicly_accessible = false`). QuickSight (edición Enterprise, prueba de 30 días)
entra por una *VPC connection* con su propio grupo de seguridad (`taller-dw-quicksight`: solo TCP 5439 hacia Redshift) y un rol IAM mínimo
(`taller-dw-quicksight-vpc`, solo acciones `ec2:*NetworkInterface`/`Describe*`). Redshift acepta 5439 únicamente desde la EC2 y desde ese grupo.
**Usuario de solo lectura:** `quicksight_ro` (SELECT solo en `marts`), contraseña aleatoria mostrada una vez; no se usa el administrador.

## Pasos
1. Terraform (Mac): `terraform apply` en `infra/terraform` (crea el grupo de seguridad, la regla de Redshift y el rol) y `terraform output` para ver VPC, subredes, grupo y rol.
2. Crear la cuenta de QuickSight (consola, región us-east-1): edición **Enterprise**, prueba gratuita.
3. *Manage QuickSight → VPC connections → Add*: nombre `taller-dw-redshift`, la VPC, las 2 subredes, el grupo `taller-dw-quicksight` y el rol `taller-dw-quicksight-vpc`. Esperar estado *Available*.
4. EC2: `./scripts/aws/ec2_dbt.sh run --select rpt_sales_dashboard` y `./scripts/aws/ec2_run.sh ecommerce_dw.loading.quicksight_user` (anota la contraseña mostrada).
5. *Datasets → New → Redshift (manual connect)*: tipo de conexión = la VPC connection; servidor = endpoint de Redshift; puerto 5439; base `dw`; usuario `quicksight_ro`; contraseña del paso anterior → *Validate*.
6. Esquema `marts` → tabla `rpt_sales_dashboard` → **Import to SPICE** → *Visualize*.
7. Visuales: **KPI** "Ventas totales" = suma de `subtotal`; **barras** `product_category` × suma de `subtotal` (color por `sales_channel`); **filtro** sobre `sale_year`/`product_category` agregado a la hoja como control.
8. *Publish dashboard*.

## Validación del indicador
`./scripts/aws/ec2_run.sh ecommerce_dw.orchestration.indicator` (SQL en `docs/aws/quicksight_validacion.sql`) imprime el total y las barras desde la misma tabla; deben coincidir con el KPI y el gráfico.

## Actualización de SPICE
SPICE es una copia: no se actualiza sola al cambiar Redshift. En *Datasets → rpt_sales_dashboard → Refresh → Add new schedule*: diario, **07:00 America/Bogota**
(el DAG corre a las 06:00 y tarda ~4 minutos). Tras una corrida manual del DAG, usar *Refresh now*. Si la actualización falla, QuickSight conserva los datos anteriores.

## Eliminación
Al terminar: borrar dashboard, análisis, dataset y VPC connection; **QuickSight → Account settings → Unsubscribe** (evita cargos al terminar la prueba); luego `terraform destroy`.
