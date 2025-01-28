import os

# import pandas as pd
import polars as pl
from dotenv import load_dotenv
from sqlalchemy import Column, MetaData, String, Table, create_engine, text

from src.logger import logger

load_dotenv()


class PostgresClient:
    # definindo argumentos padrão, se nada for definido, usará default
    def __init__(self):
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.database_name = os.getenv("PG_DATABASE")
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.schema = os.getenv("PG_SCHEMA", "public")  # Default apenas para schema
        self._engine = None
        self._connect()

    def _connect(self):
        try:
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}"
            self._engine = create_engine(connection_string)
            with self._engine.connect() as conn:
                conn.execute(text(f"SET search_path TO {self.schema}"))
            logger.info(
                f"Database '{self.database_name}' connected successfully on host '{self.host}'"
            )
        except Exception:
            logger.exception("Database connection failed")

    def save_dataframe(self, df, table_name, if_exists="append"):
        """
        Salva um DataFrame em uma tabela do PostgreSQL.
        :param df: DataFrame do pandas.
        :param table_name: Nome da tabela de destino.
        :param if_exists: Comportamento se a tabela já existir ('fail', 'replace', 'append').
        """
        try:
            df.to_sql(
                table_name,
                self._engine,
                schema=self.schema,
                if_exists=if_exists,
                index=False,
            )
            logger.info(f"Dataframe data saved succesfully in {table_name}")
        except Exception:
            logger.exception(f"Dataframe failed to save in {table_name}")

    def save_dataframe_polars(self, df, table_name, if_exists="append"):
        """
        Salva um DataFrame em uma tabela do PostgreSQL.
        :param df: DataFrame do polars.
        :param table_name: Nome da tabela de destino.
        :param if_exists: Comportamento se a tabela já existir ('fail', 'replace', 'append').
        """
        try:
            # Converte o DataFrame do polars para uma lista de dicionários
            data = df.to_dicts()
            metadata = MetaData()
            table = Table(
                table_name, metadata, schema=self.schema, extend_existing=True
            )

            with self._engine.connect() as conn:
                if not conn.dialect.has_table(conn, table_name, schema=self.schema):
                    # Cria a tabela se ela não existir
                    columns = [Column(col, String) for col in df.columns]
                    table = Table(
                        table_name,
                        metadata,
                        *columns,
                        schema=self.schema,
                        extend_existing=True,
                    )
                    metadata.create_all(self._engine)

                if if_exists == "replace":
                    conn.execute(table.delete())

                # Verifica se as colunas do DataFrame correspondem às colunas da tabela
                table_columns = [col.name for col in table.columns]
                data_columns = df.columns
                if set(data_columns) != set(table_columns):
                    raise ValueError(
                        f"Columns in DataFrame do not match columns in table {table_name}"
                    )

                # Insere os dados na tabela
                conn.execute(table.insert(), data)
            logger.info(f"Dataframe data saved successfully in {table_name}")
        except Exception as e:
            logger.exception(f"Dataframe failed to save in {table_name}: {e}")

    def execute_query(self, query, return_as_df=True):
        """
        Executa uma consulta SQL genérica e retorna os resultados.
        :param query: A consulta SQL a ser executada.
        :param return_as_df: Se True, retorna os resultados como um DataFrame.
        :return: Os resultados da consulta (lista ou DataFrame, dependendo de return_as_df).
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query))
                if return_as_df:
                    # Se solicitado, converte o resultado para DataFrame
                    columns = result.keys()
                    result_data = result.fetchall()
                    return pl.DataFrame(result_data, schema=columns)
                else:
                    # Caso contrário, retorna os resultados como lista
                    return result.fetchall()
        except Exception:
            logger.exception("Failed to execute query")
            return None

    def close_connection(self):
        try:
            if self._engine:
                self._engine.dispose()
                logger.info("Database connection closed")
        except Exception:
            logger.exception("Failed to close database connection")
