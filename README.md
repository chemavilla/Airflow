Overview: This repository serves as a comprehensive portfolio project showcasing the implementation of Apache Airflow for workflow orchestration. The project is containerized using Docker, and it leverages the Celery executor for efficient parallel execution of Directed Acyclic Graphs (DAGs).

Features: Dockerized Environment: The project is configured to run seamlessly within Docker containers, ensuring consistent environments across various systems.

Celery Executor: Utilizing the Celery executor allows for the parallel execution of tasks defined in Airflow DAGs, enhancing performance and scalability.

DAG Programming Practice: Explore various DAGs within the dags/ directory, each demonstrating different workflow scenarios. These DAGs can serve as educational resources for those learning about Airflow and DAG-based programming.

Notes: 
  - First, you need to run database migrations and create the first user account: "docker compose up airflow-init
  - Running Airflow and  start all services: "docker compose up
  - Execute commands in dockerize Airflow use: "./airflow.sh bash". 
  - More details: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
