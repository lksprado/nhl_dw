import os

import duckdb
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def load_db():
    # Configuração do banco de dados PostgreSQL
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    csv_file = "data/csv_data/processed/play_by_play.csv"
    schema_name = "nhl_raw"  # Substitua pelo nome do esquema
    table_name = "raw_play_by_play"

    # Conectar ao DuckDB e ler o CSV
    con = duckdb.connect()
    query = f"SELECT * FROM read_csv_auto('{csv_file}', all_varchar=True)"
    df = con.execute(query).fetchdf()  # Converte para um DataFrame Pandas

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema (opcional)
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    # Criar a tabela no esquema especificado
    qualified_table_name = f"{schema_name}.{table_name}"
    df.columns = [col.lower().replace(".", "_") for col in df.columns]
    escaped_columns = [f'"{col}"' for col in df.columns]
    create_table_query = f"CREATE TABLE {qualified_table_name} ({', '.join([f'{col} TEXT' for col in escaped_columns])});"
    cursor.execute(
        f"DROP TABLE IF EXISTS {qualified_table_name} CASCADE;"
    )  # Remover tabela antiga, se existir
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Tabela '{qualified_table_name}' criada com sucesso.")

    # Inserir os dados no PostgreSQL
    insert_query = f"INSERT INTO {qualified_table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))});"

    chunk_size = 10000
    for i in range(0, len(df), chunk_size):
        cursor.executemany(insert_query, df.iloc[i : i + chunk_size].values.tolist())

    conn.commit()
    print(f"{cursor.rowcount} registros inseridos com sucesso.")

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()


def copy_dataframe_to_postgres(file, table):
    # Configuração do banco de dados PostgreSQL
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    csv_file = file
    schema_name = "nhl_raw"
    table_name = table

    # Conectar ao DuckDB e ler o CSV
    con = duckdb.connect()
    query = f"SELECT * FROM read_csv_auto('{csv_file}', all_varchar=True)"
    df = con.execute(query).fetchdf()  # Converte para um DataFrame Pandas

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema (opcional)
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    # Criar a tabela no PostgreSQL
    df.columns = [col.lower().replace(".", "_") for col in df.columns]
    escaped_columns = [f'"{col}"' for col in df.columns]
    cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;")
    create_table_query = f"CREATE TABLE {schema_name}.{table_name} ({', '.join([f'{col} TEXT' for col in escaped_columns])});"
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Tabela '{schema_name}.{table_name}' criada com sucesso.")

    with open(csv_file, "r") as f:
        cursor.copy_expert(
            f"COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )
    conn.commit()
    print(f"Dados carregados com sucesso na tabela '{schema_name}.{table_name}'.")

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()


if __name__ == "__main__":
    # load_db()
    copy_dataframe_to_postgres(
        "data/csv_data/processed/play_by_play.csv", "raw_play_by_play"
    )
