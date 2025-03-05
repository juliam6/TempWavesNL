## Setup Instructions

1. Clone the repository

git clone https://github.com/juliam6/TempWavesNL.git

2. Navigate to respository

cd TempWavesNL

3. Start docker containers

docker-compose up --build -d

4. You might have to initiliaze airflow

docker exec -it airflow_webserver airflow db init

5. Access Airflow UI:

- Open [http://localhost:8080] (user: airflow, pwd: airflow)
- Enable and trigger the `tempwaves_pipeline` DAG

You might have to install a new user:

docker exec -it airflow_webserver airflow users create \
 --username airflow \
 --firstname Admin \
 --lastname User \
 --role Admin \
 --email admin@example.com \
 --password airflow

6. Access endpoints

http://localhost:8000/heatwaves
http://localhost:8000/coldwaves
http://localhost:8000/waves_timeseries
