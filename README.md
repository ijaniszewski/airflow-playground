## Running Airflow in Docker
[Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

### Initialize the database
```
docker compose up airflow-init
```

### Running Airflow
```
docker compose up
```

### Cleaning-up the environment
```
docker compose down --volumes --remove-orphans
```

### Connect to postgres (from a container)
```
psql "postgresql://$DB_USER:$DB_PWD@$DB_SERVER/$DB_NAME"

docker exec -it postgres-db psql "postgresql://postgres:password@postgres-db/spotify"

```