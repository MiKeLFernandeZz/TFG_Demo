from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.models import Variable

@dag(
    description='MLOps lifecycle',
    schedule_interval=None, 
    start_date=datetime(2025, 1, 14),
    catchup=False,
    tags=['useCase2', 'paola'],
) 
def useCase2():

    env_vars={
        "POSTGRES_USERNAME": Variable.get("POSTGRES_USERNAME"),
        "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
        "POSTGRES_DATABASE": Variable.get("POSTGRES_DATABASE"),
        "POSTGRES_HOST": Variable.get("POSTGRES_HOST"),
        "POSTGRES_PORT": Variable.get("POSTGRES_PORT"),
        "TRUE_CONNECTOR_EDGE_IP": Variable.get("CONNECTOR_EDGE_IP"),
        "TRUE_CONNECTOR_EDGE_PORT": Variable.get("IDS_EXTERNAL_ECC_IDS_PORT"),
        "TRUE_CONNECTOR_CLOUD_IP": Variable.get("CONNECTOR_CLOUD_IP"),
        "TRUE_CONNECTOR_CLOUD_PORT": Variable.get("IDS_PROXY_PORT"),
        "MLFLOW_ENDPOINT": Variable.get("MLFLOW_ENDPOINT"),
        "MLFLOW_TRACKING_USERNAME": Variable.get("MLFLOW_TRACKING_USERNAME"),
        "MLFLOW_TRACKING_PASSWORD": Variable.get("MLFLOW_TRACKING_PASSWORD"),
        "container": "docker"
    }

    volume_mount = k8s.V1VolumeMount(
        name="dag-dependencies", mount_path="/git"
    )

    init_container_volume_mounts = [
        k8s.V1VolumeMount(mount_path="/git", name="dag-dependencies")
    ]
    
    volume = k8s.V1Volume(name="dag-dependencies", empty_dir=k8s.V1EmptyDirVolumeSource())

    init_container = k8s.V1Container(
        name="git-clone",
        image="alpine/git:latest",
        command=["sh", "-c", "mkdir -p /git && cd /git && git config --global http.sslVerify false && git clone --single-branch https://pgregorio:WQtNsMiKRvDpzazX8fTS@gitlab.ikerlan.es/DAE/personal/pgregorio/robotmodel.git"],
        volume_mounts=init_container_volume_mounts
    )

    @task.kubernetes(
        image='clarusproject/dag-image:kaniko',
        name='build_image',
        task_id='build_image',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=False,
        in_cluster=True,
        get_logs=True,
        env_vars=env_vars
    )
    def build_image_task():
        import subprocess
        import logging
        
        path = '/git/robotmodel'
        endpoint = '172.16.57.20:5000/robot:test' 
 
        args = [
            "/kaniko/executor",
            f"--dockerfile={path}/Dockerfile",
            f"--context={path}",
            f"--destination={endpoint}",
            "--insecure",
            f"--cache=false",
            "--single-snapshot",
            "--snapshot-mode=time"
        ]
        result = subprocess.run(
            args,
            check=True
        )
        logging.warning(f"Kaniko executor finished with return code: {result.returncode}")

    @task.kubernetes(
        image='172.16.57.20:5000/robot:test', 
        name='test_model',
        task_id='test_model',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars=env_vars
    )
    def test_model_task():
        import requests 
        import subprocess
        import time
        import logging

        container_name = "test_api_container"
        image = "172.16.57.20:5000/robot:test" 

        start_command = f"docker run --rm -d --name {container_name} -p 5000:5000 {image}"

        try:
            subprocess.run(start_command, shell=True, check=True)
            logging.info("Contenedor API levantado.")
            
            # Esperar a que la API esté disponible
            for _ in range(12):  # Reintentos durante 1 minuto
                try:
                    response = requests.get("http://localhost:5000/health")
                    if response.status_code == 200:
                        logging.info("La API está lista.")
                        break
                except requests.ConnectionError:
                    time.sleep(5)
            else:
                logging.error("La API no se levantó a tiempo.")
                raise Exception("Timeout esperando la API.")

            # Probar la API
            url = "http://localhost:5000/getSinteticData"
            test_data = [
                {
                    "model": "model1.fmu",
                    "start_time": 0,
                    "stop_time": 10,
                    "step_size": 1,
                    "start_values": {"var1": 0, "var2": 1},
                    "output_variables": ["var1", "var2"]
                },
                {
                    "model": "model2.fmu",
                    "start_time": 0,
                    "stop_time": 10,
                    "step_size": 1,
                    "start_values": {"var1": 2, "var2": 3},
                    "output_variables": ["var1", "var2"]
                }
            ]

            results = []
            for data in test_data:
                response = requests.post(url, json=data)
                if response.status_code == 200:
                    results.append(response.json())
                else:
                    logging.error(f"Error probando el modelo: {response.text}")

            with open("/mnt/data/results.json", "w") as f:
                import json
                json.dump(results, f)

            logging.warning("Pruebas del modelo completadas.")

        finally:
            stop_command = f"docker stop {container_name}"
            subprocess.run(stop_command, shell=True, check=True)
            logging.info("Contenedor detenido.")

    @task.kubernetes(
        image='python:3.8-slim',
        name='evaluate_results',
        task_id='evaluate_results',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars=env_vars
    )
    def evaluate_results_task():
        import logging
        import json
        with open("/mnt/data/results.json", "r") as f:
            results = json.load(f)
        logging.warning(f"Evaluando resultados: {results}")

    build_image = build_image_task()
    test_model = test_model_task()
    evaluate_results = evaluate_results_task()

    build_image >> test_model >> evaluate_results

useCase2()