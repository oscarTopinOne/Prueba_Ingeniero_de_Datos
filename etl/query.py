def run_clientes_rentables(con, output_path):
    query = """
    SELECT 
    f.id_cliente,
    c.nombres,
    SUM(f.monto_transaccion) AS total_rentabilidad
    FROM 
    fact_transacciones f
    JOIN 
    dim_cliente c ON f.id_cliente = c.id_cliente
    WHERE 
    f.tipo_transaccion IN ('Compra', 'Pago', 'Depósito')
    GROUP BY 
      f.id_cliente, c.nombres
    ORDER BY 
     total_rentabilidad DESC
    LIMIT 10;
    """
    result = con.execute(query).fetchdf()
    result.to_csv(f'{output_path}/clientes_rentables.csv', index=False)

def create_view(con):
    con.execute("""
    CREATE OR REPLACE VIEW vista_transacciones_enriquecidas AS
    SELECT
      f.id_transaccion,
      f.fecha_hora,
      f.monto_transaccion,
      f.tipo_transaccion,
      f.numero_cuenta,
      c.nombres,
      c.numero_identificacion,
      p.tipo_producto,
      p.categoria_producto,
      r.reporte_centrales_riesgo,
      r.monto_reporte_central_riesgo,
      r.tiempo_mora_reporte_riesgo,
      d.fecha,
      d.año,
      d.mes,
      d.día,
      d.día_semana
    FROM fact_transacciones f
    JOIN dim_cliente c ON f.id_cliente = c.id_cliente
    JOIN dim_producto p ON f.id_producto = p.id_producto
    JOIN dim_riesgo r ON f.id_riesgo = r.id_riesgo
    JOIN dim_fecha d ON f.id_fecha = d.id_fecha;
    """)