This file contains several service definitions:

airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.

airflow-webserver - The webserver is available at http://localhost:8080.

airflow-worker - The worker that executes the tasks given by the scheduler.

airflow-init - The initialization service.

flower - The flower app for monitoring the environment. It is available at http://localhost:5555.

postgres - The database.

redis - The redis - broker that forwards messages from scheduler to worker.