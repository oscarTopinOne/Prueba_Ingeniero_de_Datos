import pandas as pd
import os
import unicodedata

def ingest_data(input_path, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Leer y normalizar columnas
    df = pd.read_csv(input_path, encoding='utf-8')
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')  # elimina tildes
        .strip().lower().replace(' ', '_').replace('-', '_').replace('_de_','_').replace('_del_','_').replace('_en_','_') for col in df.columns
    ]
    
    # Guardar versión limpia
    df.to_csv(output_path, index=False)
    print(f"📁 Data ingestada guardada en: {output_path}")
    return df
