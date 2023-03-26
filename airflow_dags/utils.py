import logging
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


def create_sql_hook():
    sql_hook = PostgresHook(postgres_conn_id="pet_project")
    return sql_hook


def check_data_exist(table, ds):
    logging.info(f"checking data from {table} where {ds}")
    query = f"select count(*) from {table} where dt = '{ds}'"
    res = get_data_from_db(query).iloc[0, 0]
    print(res)
    if res == 0:
        return False
    else:
        return True


def delete_data_from_db(table, ds):
    logging.info(f"start delete data from {table} where {ds}")
    query = f"delete from {table} where dt = '{ds}'"
    create_sql_hook().run(query)
    logging.info("delete complited")


def get_data_from_db(query):
    logging.info(f"started loading data with {query}")
    sql_hook = create_sql_hook()
    engine = sql_hook.get_sqlalchemy_engine()
    df = pd.read_sql_query(query, engine)
    logging.info("loading data complited")
    return df


def sql_reader(file_name):
    with open(f"/usr/local/airflow/dags/sql_queries/{file_name}") as f:
        query = " ".join(f.readlines()).replace("\n", "").replace("\t", "")
    return query


def insert_data_to_postgres(df, table):
    logging.info(f"inserting data to {table}")
    sql_hook = create_sql_hook()
    insert_rows = [tuple(row) for row in df.values]
    sql_hook.insert_rows(table=table, rows=insert_rows, commit_every=100, replace=False)
    logging.info("insertion completed")
