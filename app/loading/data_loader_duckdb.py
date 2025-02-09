import os

import duckdb
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def create_and_load_table_with_sk(file, table, sk: list):
    """Only for first load. Define a column to be used as id and unique"""
    # Configuração do banco de dados PostgreSQL
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    schema_name = "nhl_raw"
    table_name = table

    # Conectar ao DuckDB e preparar os dados
    con = duckdb.connect()
    query = f"CREATE VIEW temp_view AS SELECT * FROM read_csv_auto('{file}', all_varchar=True);"
    con.execute(query)

    # Obter os nomes das colunas diretamente do DuckDB
    col_names = [row[0] for row in con.execute("DESCRIBE temp_view").fetchall()]
    col_names = [col.lower().replace(".", "_") for col in col_names]

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    sk_concat = " || '_' || ".join(
        [f"COALESCE(REPLACE({col}, '.0', ''),'')" for col in sk] + ["filename"]
    )

    # Criar tabela com surrogate key e composite key (não adiciona filename na criação)
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'"{col}" TEXT' for col in col_names])},
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sk TEXT GENERATED ALWAYS AS (
                MD5({sk_concat})
            ) STORED, UNIQUE (sk)
        );
    """

    cursor.execute(create_table_query)
    conn.commit()

    # Exportar os dados do DuckDB para um arquivo temporário e carregar no PostgreSQL
    temp_output = "/tmp/temp_duckdb_output.csv"
    con.execute(
        f"COPY (SELECT * FROM temp_view) TO '{temp_output}' WITH (HEADER, DELIMITER ',')"
    )

    # Agora, carregar os dados do arquivo temporário no PostgreSQL
    with open(temp_output, "r") as f:
        cursor.copy_expert(
            f"COPY {table_name} ({', '.join([f'\"{col}\"' for col in col_names])}) FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )

    conn.commit()
    print(f"Tabela '{schema_name}.{table_name}' criada e dados carregados com sucesso.")

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()


def create_and_load_table_with_pk(file, table, pk):
    """Only for first load. Define a column to be used as id and unique"""
    # Configuração do banco de dados PostgreSQL
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    schema_name = "nhl_raw"
    table_name = table

    # Conectar ao DuckDB e preparar os dados
    con = duckdb.connect()
    query = f"CREATE VIEW temp_view AS SELECT * FROM read_csv_auto('{file}', all_varchar=True);"
    con.execute(query)

    # Obter os nomes das colunas diretamente do DuckDB
    col_names = [row[0] for row in con.execute("DESCRIBE temp_view").fetchall()]
    col_names = [col.lower().replace(".", "_") for col in col_names]

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    # Criar tabela com surrogate key e composite key (não adiciona filename na criação)
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'"{col}" TEXT' for col in col_names])},
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE ({pk})
        );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Exportar os dados do DuckDB para um arquivo temporário e carregar no PostgreSQL
    temp_output = "/tmp/temp_duckdb_output.csv"
    con.execute(
        f"COPY (SELECT * FROM temp_view) TO '{temp_output}' WITH (HEADER, DELIMITER ',')"
    )

    # Agora, carregar os dados do arquivo temporário no PostgreSQL
    with open(temp_output, "r") as f:
        cursor.copy_expert(
            f"COPY {table_name} ({', '.join([f'\"{col}\"' for col in col_names])}) FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )

    conn.commit()
    print(f"Tabela '{schema_name}.{table_name}' criada e dados carregados com sucesso.")

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()


def update_table_with_sk(file, table, sk: list):
    """
    Cria uma tabela temporária, carrega dados nela e faz um UPSERT na tabela original.
    Define uma coluna para ser usada como ID.
    """
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    schema_name = "nhl_raw"
    table_name = table
    temp_table_name = f"{table}_temp"

    # Conectar ao DuckDB e preparar os dados
    con = duckdb.connect()
    query = f"CREATE VIEW temp_view AS SELECT * FROM read_csv_auto('{file}', all_varchar=True);"
    con.execute(query)

    # Obter os nomes das colunas diretamente do DuckDB
    col_names = [row[0] for row in con.execute("DESCRIBE temp_view").fetchall()]
    col_names = [col.lower().replace(".", "_") for col in col_names]

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    sk_concat = " || '_' || ".join(
        [f"COALESCE(REPLACE({col}, '.0', ''),'')" for col in sk] + ["filename"]
    )

    # Criar a tabela temporária
    create_temp_table_query = f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} (
            {', '.join([f'"{col}" TEXT' for col in col_names])},
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sk TEXT GENERATED ALWAYS AS (
                MD5({sk_concat})
            ) STORED, UNIQUE (sk)
        );
    """
    cursor.execute(create_temp_table_query)
    conn.commit()

    # Exportar os dados do DuckDB para um arquivo temporário e carregar no PostgreSQL
    temp_output = "/tmp/temp_duckdb_output.csv"
    con.execute(
        f"COPY (SELECT * FROM temp_view) TO '{temp_output}' WITH (HEADER, DELIMITER ',')"
    )

    # Carregar os dados do arquivo temporário na tabela temporária do PostgreSQL
    with open(temp_output, "r") as f:
        cursor.copy_expert(
            f"COPY {temp_table_name} ({', '.join([f'\"{col}\"' for col in col_names])}) FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )
    conn.commit()

    # Realizar o UPSERT na tabela original
    upsert_query = f"""
    INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in col_names])})
    SELECT {', '.join([f'"{col}"' for col in col_names])}
    FROM {temp_table_name}
    ON CONFLICT (sk)
    DO UPDATE SET
    {', '.join([f'{col} = EXCLUDED.{col}' for col in col_names if col != f'{sk_concat}'])};
    """
    cursor.execute(upsert_query)
    conn.commit()

    print(
        f"Tabela temporária '{schema_name}.{temp_table_name}' criada, dados carregados e UPSERT feito na tabela '{table_name}' com sucesso."
    )

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()


def update_table_with_pk(file, table, pk):
    """
    Cria uma tabela temporária, carrega dados nela e faz um UPSERT na tabela original.
    Define uma coluna para ser usada como ID.
    """
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    schema_name = "nhl_raw"
    table_name = table
    temp_table_name = f"{table}_temp"

    # Conectar ao DuckDB e preparar os dados
    con = duckdb.connect()
    query = f"CREATE VIEW temp_view AS SELECT * FROM read_csv_auto('{file}', all_varchar=True);"
    con.execute(query)

    # Obter os nomes das colunas diretamente do DuckDB
    col_names = [row[0] for row in con.execute("DESCRIBE temp_view").fetchall()]
    col_names = [col.lower().replace(".", "_") for col in col_names]

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Definir o esquema
    cursor.execute(f"SET search_path TO {schema_name};")
    conn.commit()

    # Criar a tabela temporária
    create_temp_table_query = f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} (
            {', '.join([f'"{col}" TEXT' for col in col_names])},
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE ({pk})
        );
    """
    cursor.execute(create_temp_table_query)
    conn.commit()

    # Exportar os dados do DuckDB para um arquivo temporário e carregar no PostgreSQL
    temp_output = "/tmp/temp_duckdb_output.csv"
    con.execute(
        f"COPY (SELECT * FROM temp_view) TO '{temp_output}' WITH (HEADER, DELIMITER ',')"
    )

    # Carregar os dados do arquivo temporário na tabela temporária do PostgreSQL
    with open(temp_output, "r") as f:
        cursor.copy_expert(
            f"COPY {temp_table_name} ({', '.join([f'\"{col}\"' for col in col_names])}) FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )
    conn.commit()

    # Realizar o UPSERT na tabela original
    upsert_query = f"""
    INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in col_names])})
    SELECT {', '.join([f'"{col}"' for col in col_names])}
    FROM {temp_table_name}
    ON CONFLICT ({pk})
    DO UPDATE SET
    {', '.join([f'{col} = EXCLUDED.{col}' for col in col_names if col != f'{pk}'])};
    """
    cursor.execute(upsert_query)
    conn.commit()

    print(
        f"Tabela temporária '{schema_name}.{temp_table_name}' criada, dados carregados e UPSERT feito na tabela '{table_name}' com sucesso."
    )

    # Fechar conexões
    cursor.close()
    conn.close()
    con.close()
