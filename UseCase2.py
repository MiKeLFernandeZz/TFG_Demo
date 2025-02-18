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
        endpoint = '172.16.57.20:5000/robot:test'  # Cambia esto si el registro está en otra dirección
 
        args = [
            "/kaniko/executor",
            f"--dockerfile={path}/Dockerfile",
            f"--context={path}",
            f"--destination={endpoint}",
            "--insecure",
            "--cache=true",
            "--cache-repo=172.16.57.20:5000/cache"
        ]
        result = subprocess.run(
            args,
            check=True
        )
        logging.warning(f"Kaniko executor finished with return code: {result.returncode}")


    build_image = build_image_task()

    build_image

useCase2()