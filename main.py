import shutil
import os
import duckdb
from etl.ingest import ingest_data
from etl.transform import clean_data, validate_schema, remove_duplicates
from etl.quality import validar_calidad_total, descartar_registros_con_alerta
from etl.dimensions import build_dim_cliente, build_dim_producto, build_dim_riesgo, build_dim_fecha
from etl.load import load_to_duckdb
from etl.export import export_to_csv
from etl.query import run_clientes_rentables, create_enriched_view

DATA_PATH = 'data/origen/datos_transacciones.csv'
INGESTED_PATH = 'data/preparado/data_ingestada.csv'
OUTPUT_PATH = 'data/output'
DB_PATH = 'data_pipeline.duckdb'  # Archivo que Grafana lee
#DB_TEMP_PATH = 'data_pipeline_temp.duckdb'  # Archivo temporal para escritura

def main():
    print("🔄 Ingestando datos...")
    df = ingest_data(DATA_PATH, INGESTED_PATH)

    print("✅ Validando esquema...")
    validate_schema(df)

    print("🧹 Limpiando datos y removiendo duplicados...")
    df = clean_data(df)
    df = remove_duplicates(df)

    print("📋 Validando calidad de datos...")
    df = validar_calidad_total(df)
    df = descartar_registros_con_alerta(df)


    print("🧩 Construyendo dimensiones...")
    dim_cliente = build_dim_cliente(df)
    dim_producto = build_dim_producto(df)
    dim_riesgo = build_dim_riesgo(df)
    dim_fecha = build_dim_fecha(df)
    

    print(df.columns)

    print("🔗 Enlazando claves...")

    # 🗓️ Normaliza fecha para unir con dim_fecha
    df['fecha_transaccion'] = df['fecha_hora'].dt.normalize()

    # 👤 Enlaza con dim_cliente usando columnas descriptivas
    df = df.merge(dim_cliente[['id_cliente', 'tipo_identificacion', 'numero_identificacion', 'nombres']],
              on=['tipo_identificacion', 'numero_identificacion', 'nombres'], how='left')

    # 📦 Enlaza con dim_producto usando tipo_producto
    df = df.merge(dim_producto[['id_producto', 'tipo_producto']],
              on='tipo_producto', how='left')

    # ⚠️ Enlaza con dim_riesgo_crediticio usando reporte, monto y tiempo
    df = df.merge(dim_riesgo[['id_riesgo', 'reporte_centrales_riesgo', 'monto_reporte_central_riesgo', 'tiempo_mora_reporte_riesgo']],
              on=['reporte_centrales_riesgo', 'monto_reporte_central_riesgo', 'tiempo_mora_reporte_riesgo'], how='left')

    # 📅 Enlaza con dim_fecha usando fecha normalizada
    df = df.merge(dim_fecha[['id_fecha', 'fecha']],
              left_on='fecha_hora', right_on='fecha', how='left')


    print("📦 Construyendo tabla de hechos...")
    df['id_transaccion'] = df.index + 1
    fact = df[['id_transaccion', 'fecha_hora', 'monto_transaccion', 'tipo_transaccion',
               'numero_cuenta', 'id_cliente', 'id_producto', 'id_riesgo', 'id_fecha',
               'alerta_fecha_futura', 'alerta_monto_negativo', 'alerta_tipo_desconocido']]

    print("🛢️ Cargando en DuckDB...")
    con = duckdb.connect(DB_PATH)
    load_to_duckdb(con, 'fact_transacciones', fact, cluster_columns=['id_cliente', 'tipo_transaccion', 'id_producto'])
    load_to_duckdb(con, 'dim_cliente', dim_cliente)
    load_to_duckdb(con, 'dim_producto', dim_producto)
    load_to_duckdb(con, 'dim_riesgo', dim_riesgo)
    load_to_duckdb(con, 'dim_fecha', dim_fecha)


    print("📤 Exportando CSVs...")
    for table in ['fact_transacciones', 'dim_cliente', 'dim_producto', 'dim_riesgo', 'dim_fecha']:
        export_to_csv(con, table, OUTPUT_PATH)

    print("📊 Ejecutando vista enriquecida...")
    create_enriched_view(con)
    run_clientes_rentables(con, OUTPUT_PATH)

    print("Pipeline completado con éxito.")

    print(con.execute("SELECT * FROM vista_transacciones_enriquecidas").fetchdf())

    con.close()


if __name__ == "__main__":
    main()
