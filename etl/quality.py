import pandas as pd
import re


def normalizar_fecha(col, incluir_hora=False):
    def convertir(valor):
        if pd.isna(valor):
            return pd.NaT
        valor = str(valor).strip()

        # Si es un número de 10 dígitos → Unix timestamp
        if re.match(r'^\d{10}$', valor):
            return pd.to_datetime(int(valor), unit='s', errors='coerce')

        # Si parece fecha legible → texto tipo yyyy-mm-dd
        return pd.to_datetime(valor, errors='coerce')

    fechas = col.apply(convertir)
    return fechas if incluir_hora else fechas.dt.normalize()


# Validar fecha de nacimiento (entre 1920 y hoy)
def validar_fecha_nacimiento(df, col='fecha_nacimiento'):
    hoy = pd.Timestamp.today().normalize()
    df[col] = normalizar_fecha(df[col])
    df['alerta_fecha_nacimiento'] = df[col].isna() | (df[col] < '1900-01-01') | (df[col] > hoy)
    return df

# Validar fecha-hora de transacción (no futura)
def validar_fecha_transaccion(df, col='fecha_hora'):
    df[col] = normalizar_fecha(df[col], incluir_hora=True)
    df['alerta_fecha_futura'] = df[col].notna() & (df[col] > pd.Timestamp.now())
    return df

def validar_correo(df, col='correo_cliente'):
    # Normaliza texto
    df[col] = df[col].astype(str).str.strip().str.lower()

    # Reemplaza correos ficticios por cadena vacía
    correos_ficticios = ['sincorreo', 'correo@', '@dominio.com', 'usuario@dominio', 'correo@.', '@dominio']
    df[col] = df[col].replace(correos_ficticios, '')

    # Valida formato solo si no está vacío
    regex_valido = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    df['alerta_correo_invalido'] = df[col].ne('') & ~df[col].str.match(regex_valido, na=False)

    return df


# Validar teléfono (mínimo 7 dígitos)
def validar_telefono(df, col='telefono_cliente'):
    df['alerta_telefono_invalido'] = ~df[col].astype(str).str.replace(r'\D', '', regex=True).str.len().ge(7)
    return df

# Validar dirección no vacía
def validar_direccion(df, col='direccion_cliente'):
    df['alerta_direccion_invalida'] = df[col].isna() | df[col].astype(str).str.strip().eq('')
    return df

# Validar monto no negativo
def validar_monto(df, col='monto_transaccion'):
    df['alerta_monto_negativo'] = pd.to_numeric(df[col], errors='coerce') < 0
    return df

# Validar tipo de transacción
def validar_tipo_transaccion(df, col='tipo_transaccion'):
    validos = ['Compra', 'Pago', 'Depósito', 'Transferencia', 'Retiro']
    df[col] = df[col].astype(str).str.capitalize().str.strip()
    df['alerta_tipo_desconocido'] = ~df[col].isin(validos)
    return df

# Aplicar todas las validaciones
def validar_calidad_total(df):
    df = validar_fecha_nacimiento(df)
    df = validar_fecha_transaccion(df)
    df = validar_correo(df)
    df = validar_telefono(df)
    df = validar_direccion(df)
    df = validar_monto(df)
    df = validar_tipo_transaccion(df)

    # Resumen de alertas
    alertas = [col for col in df.columns if col.startswith('alerta_')]
    df['total_alertas'] = df[alertas].sum(axis=1)
    df_invalidos = df[df['total_alertas'] > 0]
    df_invalidos.to_csv('data/preparado/registros_invalidos.csv', index=False)

    print("Resumen de alertas:")
    print(df[alertas].sum().sort_values(ascending=False))
    print(f"\nTotal registros con al menos una alerta: {len(df_invalidos)}")
    

    return df

def descartar_registros_con_alerta(df):
    alertas = [col for col in df.columns if col.startswith('alerta_')]
    return df[~df[alertas].any(axis=1)]
