"""Consulta SQL que valida el indicador y el gráfico del dashboard de QuickSight (misma tabla que consume el dashboard).

  ./scripts/aws/ec2_run.sh ecommerce_dw.orchestration.indicator
Compara estos valores con el KPI "Ventas totales" y con las barras del dashboard.
"""

from ecommerce_dw.loading.redshift_data_api import DataApi

TOTAL_SQL = "SELECT SUM(subtotal) AS ventas_totales, COUNT(*) AS lineas FROM marts.rpt_sales_dashboard"
FACT_SQL = "SELECT SUM(subtotal) FROM marts.fct_sales"
BY_CATEGORY_SQL = """
SELECT COALESCE(product_category, '(sin categoría)') AS categoria, SUM(subtotal) AS ventas
FROM marts.rpt_sales_dashboard GROUP BY 1 ORDER BY 2 DESC
"""
BY_CHANNEL_SQL = "SELECT sales_channel, SUM(subtotal) AS ventas FROM marts.rpt_sales_dashboard GROUP BY 1 ORDER BY 2 DESC"


def main(api=None):
    api = api or DataApi()
    total, lines = api.execute(TOTAL_SQL)[0]
    fact = api.execute(FACT_SQL)[0][0]
    print(f"Indicador 'Ventas totales' (rpt_sales_dashboard): {float(total):.2f}  ({lines} líneas)")
    print(f"Control: suma de marts.fct_sales:                {float(fact):.2f}  -> {'COINCIDE' if abs(float(total) - float(fact)) < 0.01 else 'NO COINCIDE'}")
    for title, sql in (("Ventas por categoría", BY_CATEGORY_SQL), ("Ventas por canal", BY_CHANNEL_SQL)):
        print(f"\n{title}")
        for name, value in api.execute(sql):
            print(f"  {name:<22}{float(value):>14.2f}")


if __name__ == "__main__":
    main()
