from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
from airflow.operators.email_operator import EmailOperator
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import utils
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dieshit495@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 11, 3),
}

args = {
    "owner": "dima",
    "start_date": datetime(2021, 11, 3),
    "provide_context": True,
}

mail = "<dieshit495@gmail.com>"


def parse_values(**kwargs):
    logging.info("starting of parsing data")
    ds = kwargs["ds"]
    table = kwargs["table"]
    date_for_url = ds.replace("-", ".")
    date_for_url = date_for_url[5:] + date_for_url[4:5] + date_for_url[0:4]
    URL = f"""https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&
              UniDbQuery.To={date_for_url}"""
    response = requests.get(URL).text
    soup = BeautifulSoup(response, "html.parser")
    pars = soup.find("div", class_="table-wrapper")
    df = pd.DataFrame(columns=["a", "b", "c", "d", "e", "f"])
    index = 0
    logging.info("starting creating df")
    for row in pars.find_all("tr")[1:]:
        row = row.find_all("td")
        row = [i.text for i in row]
        row[-1] = float(row[-1].replace(",", "."))
        df.loc[index] = [ds] + row
        index += 1
    logging.info("df was created")
    if utils.check_data_exist(table, ds):
        utils.delete_data_from_db(table, ds)
    utils.insert_data_to_postgres(df, table)


def avg_value(file_name):
    query = utils.sql_reader(file_name)
    avg_value = utils.get_data_from_db(query)
    avg_value = avg_value.iloc[0, 0]
    return avg_value


def ploter(**kwargs):
    logging.info("drawing data plot")
    file_name = kwargs["file_name"]
    query = utils.sql_reader(file_name)
    data_for_plot = utils.get_data_from_db(query)
    plt.rcParams["figure.figsize"] = (40, 25)
    plt.style.use("dark_background")
    sns.lineplot(x="alfcode", y="rate", data=data_for_plot)
    plt.xticks(rotation=45)
    logging.info("saving data plot")
    plt.savefig("values_plot.png")

file_name_for_send_in_email = "count_avg_value.sql"

with DAG(
    "Pet_project",
    description="email_of_parsing_values_avg_and_plot",
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args=args,
) as dag:  # 0 * * * *   */1 * * * *

    parser = PythonOperator(
        task_id="parser",
        python_callable=parse_values,
        op_kwargs={"table": "pet_project_rate_values"},
    )

    plot = PythonOperator(
        task_id="ploter",
        python_callable=ploter,
        op_kwargs={"file_name": "select_all_data.sql"},
    )

    send_email_notification = EmailOperator(
        task_id="send_test_email",
        to=mail,
        subject="Value plot and avg",
        html_content=f"""average rate =
        {avg_value(file_name_for_send_in_email)}""".encode(
            "utf-8"
        ),
        files=["values_plot.png"],
    )

    parser >> plot >> send_email_notification
