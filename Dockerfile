FROM apache/airflow:1.10.10

RUN pip install requests

ENTRYPOINT ["sh","-c"]

CMD "airflow initdb && airflow scheduler -D && airflow webserver -p 8080"