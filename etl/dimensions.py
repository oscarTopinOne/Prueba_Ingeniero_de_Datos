import pandas as pd

def build_dim_cliente(df):
    dim = df[['tipo_identificacion', 'numero_identificacion', 'nombres',
              'fecha_nacimiento', 'direccion_cliente', 'telefono_cliente',
              'correo_cliente', 'ciudad']].drop_duplicates().reset_index(drop=True)
    dim['id_cliente'] = dim.index + 1
    return dim

def build_dim_producto(df):
    productos = df['tipo_producto'].dropna().unique()
    dim = pd.DataFrame({
        'tipo_producto': productos,
        'categoria_producto': ['Depósito' if 'cuenta' in p.lower() else 'Crédito' if 'crédito' in p.lower() else 'Inversión' for p in productos],
        'descripcion': [f'Producto financiero tipo {p}' for p in productos]
    })
    dim['id_producto'] = dim.index + 1
    return dim

def build_dim_riesgo(df):
    dim = df[['reporte_centrales_riesgo', 'monto_reporte_central_riesgo', 'tiempo_mora_reporte_riesgo']].drop_duplicates().reset_index(drop=True)
    dim['id_riesgo'] = dim.index + 1
    return dim

def build_dim_fecha(df):
    fechas = pd.to_datetime(df['fecha_hora'].dropna().unique())
    dim = pd.DataFrame({
        'fecha': fechas,
        'año': fechas.year,
        'mes': fechas.month,
        'día': fechas.day,
        'día_semana': fechas.day_name()
    })
    dim['id_fecha'] = dim.index + 1
    return dim