from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests, json
from bs4 import BeautifulSoup

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com'
}

def scrapper():
    url = "http://www.ufcstats.com/statistics/fighters"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    fighters_data = []

    # Get the links to each fighter's page
    fighter_links = []
    for a in soup.select("td.b-statistics__table-col a"):
        link = a["href"]
        if "fighter-details" in link:
            fighter_links.append(link)

    # Iterate through each fighter's page and extract the data you need
    for link in fighter_links:
        fighter_page = requests.get(link)
        fighter_soup = BeautifulSoup(fighter_page.content, "html.parser")
        name = fighter_soup.select_one(".b-content__title-highlight").text
        nickname = fighter_soup.select_one(".b-content__Nickname").text
        record = fighter_soup.select_one(".b-content__title-record").text

        stats = fighter_soup.find_all("li", {"class": "b-list__box-list-item"})
        
        data = {
            'name': name.replace('\n', ''),
            'nickname': nickname.replace('\n', ''),
            'record': record.replace('\n', '')
        }

        for stat in stats:
            text = stat.text.replace('\n', '').replace(' ', '')
            if len(text) > 1:
                key, value = text.split(":")[0], text.split(":")[1]
                data[key] = value

        json_data = json.dumps(data)
        fighters_data.append(json_data)

    context['ti'].xcom_push(key='scraped_data', value=scraped_data)

with DAG(dag_id='parallel_dag', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    scrapper