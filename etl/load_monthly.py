#!/usr/bin/env python3
"""
Carga mensual de campañas SCB desde GitLab parquet a PostgreSQL
Descarga BD_CAMP_SCP_YYYYMM.parquet y lo carga en stga_scp.maestra_campanias_scb_mes
"""
import os
import sys
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql


def get_env_var(name: str) -> str:
    """Obtiene variable de entorno o falla con mensaje claro"""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variable de entorno {name} no configurada")
    return value


def download_parquet_from_gitlab(year_month: str) -> pd.DataFrame:
    """
    Descarga parquet desde GitLab usando Personal Access Token
    
    Args:
        year_month: formato YYYYMM (ej: 202505)
    
    Returns:
        DataFrame con los datos del parquet
    """
    gitlab_token = get_env_var("GITLAB_TOKEN")
    gitlab_project_id = get_env_var("GITLAB_PROJECT_ID")  # ej: da1534036%2Fcompartido_temporal
    
    filename = f"BD_CAMP_SCP_{year_month}.parquet"
    
    # URL de GitLab API para descargar archivo raw
    url = f"https://gitlab.com/api/v4/projects/{gitlab_project_id}/repository/files/{filename}/raw?ref=main"
    
    headers = {"PRIVATE-TOKEN": gitlab_token}
    
    print(f"Descargando {filename} desde GitLab...")
    response = requests.get(url, headers=headers, timeout=300)
    
    if response.status_code != 200:
        raise Exception(f"Error descargando parquet: {response.status_code} - {response.text}")
    
    # Guardar temporalmente
    temp_path = f"/tmp/{filename}"
    with open(temp_path, "wb") as f:
        f.write(response.content)
    
    print(f"Archivo descargado: {len(response.content)} bytes")
    
    # Leer parquet
    df = pd.read_parquet(temp_path)
    
    # Limpiar archivo temporal
    os.remove(temp_path)
    
    print(f"DataFrame cargado: {len(df)} filas, {len(df.columns)} columnas")
    return df


def load_to_postgresql(df: pd.DataFrame, table_name: str = "stga_scp.maestra_campanias_scb_mes"):
    """
    Carga DataFrame a PostgreSQL, reemplazando tabla completa
    
    Args:
        df: DataFrame con los datos
        table_name: nombre completo de la tabla (schema.table)
    """
    # Configuración de conexión
    conn_params = {
        "host": get_env_var("DB_HOST"),
        "port": int(get_env_var("DB_PORT")),
        "database": get_env_var("DB_NAME"),
        "user": get_env_var("DB_USER"),
        "password": get_env_var("DB_PASSWORD"),
        "sslmode": get_env_var("DB_SSLMODE"),
    }
    
    print(f"Conectando a PostgreSQL {conn_params['host']}:{conn_params['port']}...")
    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        # Separar schema y tabla
        schema, table = table_name.split(".")
        
        # Crear schema si no existe
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        
        # Drop table si existe
        print(f"Eliminando tabla existente {table_name}...")
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        ))
        
        # Crear tabla desde DataFrame
        print(f"Creando tabla {table_name}...")
        
        # Mapeo de tipos pandas a PostgreSQL
        dtype_map = {
            "object": "TEXT",
            "int64": "BIGINT",
            "float64": "DOUBLE PRECISION",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }
        
        columns = []
        for col, dtype in df.dtypes.items():
            pg_type = dtype_map.get(str(dtype), "TEXT")
            columns.append(f'"{col}" {pg_type}')
        
        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        cursor.execute(create_table_sql)
        
        # Insertar datos en batch
        print(f"Insertando {len(df)} filas...")
        
        cols = df.columns.tolist()
        cols_str = ", ".join([f'"{col}"' for col in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        
        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in df.to_numpy()]
        
        # Insertar en batch de 1000 filas
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            if (i + batch_size) % 10000 == 0:
                print(f"  Insertadas {i + batch_size} filas...")
        
        # Crear índice en nro_documento si existe
        if "nro_documento" in df.columns:
            print("Creando índice en nro_documento...")
            index_name = f"idx_{table}_nro_documento"
            cursor.execute(sql.SQL("CREATE INDEX {} ON {}.{} (nro_documento)").format(
                sql.Identifier(index_name),
                sql.Identifier(schema),
                sql.Identifier(table)
            ))
        
        conn.commit()
        print(f"✅ Carga completada: {len(df)} filas en {table_name}")
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error en carga a PostgreSQL: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()


def main():
    """Función principal - ejecuta la carga mensual"""
    try:
        # Determinar año-mes (usa variable de entorno o mes anterior)
        year_month = os.getenv("YEAR_MONTH")
        
        if not year_month:
            # Si no se especifica, usar mes anterior
            now = datetime.now()
            if now.month == 1:
                year_month = f"{now.year - 1}12"
            else:
                year_month = f"{now.year}{now.month - 1:02d}"
        
        print(f"=== Iniciando carga mensual SCB para {year_month} ===")
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        # Descargar parquet
        df = download_parquet_from_gitlab(year_month)
        
        # Cargar a PostgreSQL
        load_to_postgresql(df)
        
        print(f"=== Carga mensual completada exitosamente ===")
        return 0
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
