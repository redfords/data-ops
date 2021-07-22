FROM apache/airflow:1.10.10

ENV AIRFLOW_HOME="/home/airflow"

RUN pip install -r requirements.txt 

RUN mkdir /home/airflow/dags

COPY movie_dag.py   \
     extract.py     \
     transform.py   \
     /home/airflow/dags