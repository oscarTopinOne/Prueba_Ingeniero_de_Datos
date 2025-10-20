import pandas as pd


def validate_schema(df):
    expected_columns = [
        'tipo_identificacion','numero_identificacion','numero_cuenta','nombres','tipo_transaccion',
        'monto_transaccion','tipo_producto','ciudad','fecha_hora','fecha_nacimiento','direccion_cliente','telefono_cliente',
        'correo_cliente','reporte_centrales_riesgo','monto_reporte_central_riesgo','tiempo_mora_reporte_riesgo'
    ]
    missing = [col for col in expected_columns if col not in df.columns]
    print("🧾 Columnas detectadas:")
    print(df.columns.tolist())

    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
    return True

def clean_data(df):
    df['monto_transaccion'] = pd.to_numeric(df['monto_transaccion'], errors='coerce')
    df['tipo_transaccion'] = df['tipo_transaccion'].str.capitalize().str.strip().str.replace('-', '_')
    df.fillna({
        'tipo_transaccion': 'otro',
        'reporte_central_riesgo': 'sin_reporte',
        'tiempo_mora_reporte_riesgo': 'sin_mora'
    }, inplace=True)
    return df

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"🔍 Duplicados eliminados: {before - after}")
    return df
