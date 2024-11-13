from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.models import Variable

@dag(
    description='Demo',
    schedule_interval=None, 
    start_date=datetime(2024, 11, 12),
    catchup=False,
    tags=['demo', 'redwine', 'mlflow', 'production'],
)
def Redwine_production_example():

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
        "MLFLOW_TRACKING_URI": Variable.get("MLFLOW_ENDPOINT"),
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
        command=["sh", "-c", "mkdir -p /git && cd /git && git clone -b main --single-branch https://github.com/MiKeLFernandeZz/TFG_Demo.git"],
        volume_mounts=init_container_volume_mounts
    )
    
    pod_spec = k8s.V1Pod(
        api_version='v1',
        kind='Pod',
        spec=k8s.V1PodSpec(
            runtime_class_name='nvidia',  # Establecer runtimeClassName a 'nvidia'
            containers=[
                k8s.V1Container(
                    name='base',
                )
            ]
        )
    )

    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='read_df',
        task_id='read_df',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '0.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def read_df_task():
        import sys
        import redis
        import uuid
        import pickle
    
        sys.path.insert(1, '/git/TFG_Demo/src/redwine')
        from Data.read_data import read_data
        
        """
        MODIFY WHAT YOU WANT
        """
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
    
        df = read_data()
        
        df_id = str(uuid.uuid4())
        redis_client.set(df_id, pickle.dumps(df))
    
        return df_id
    
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='process_df',
        task_id='process_df',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '0.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def process_df_task(df_id):
        import sys
        import redis
        import uuid
        import pickle
    
        sys.path.insert(1, '/git/TFG_Demo/src/redwine')
        from Process.data_processing import data_processing
        
        """
        MODIFY WHAT YOU WANT
        """
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
        df = pickle.loads(redis_client.get(df_id))
    
        dp = data_processing(df)
        
        dp_id = str(uuid.uuid4())
        redis_client.set(dp_id, pickle.dumps(dp))
    
        return dp_id
    
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='train_elastic',
        task_id='train_elastic',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '0.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def train_elastic_task(dp_id):
        import sys
        import redis
        import uuid
        import pickle

        import mlflow
        import os
    
        sys.path.insert(1, '/git/TFG_Demo/src/redwine')
        from Models.ElasticNet_model_training import elasticNet_model_training
        
        """
        MODIFY WHAT YOU WANT
        """
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
        dp = pickle.loads(redis_client.get(dp_id))

        # mlflow_endpoint = os.getenv("MLFLOW_ENDPOINT")
        # mlflow_experiment = "redwine_production_test"

        # mlflow.set_tracking_uri(mlflow_endpoint)
        # mlflow.set_experiment(mlflow_experiment)
        # mlflow.autolog()

        # with mlflow.start_run():
        return elasticNet_model_training(dp)
        
        
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='SVC_train',
        task_id='SVC_train',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '0.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def SVC_train_task(dp_id):
        import sys
        import redis
        import uuid
        import pickle

        import mlflow
        import os
    
        sys.path.insert(1, '/git/TFG_Demo/src/redwine')
        from Models.SVC_model_training import svc_model_training
        
        """
        MODIFY WHAT YOU WANT
        """
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
        dp = pickle.loads(redis_client.get(dp_id))

        # mlflow_endpoint = os.getenv("MLFLOW_ENDPOINT")
        # mlflow_experiment = "redwine_production_test"

        # mlflow.set_tracking_uri(mlflow_endpoint)
        # mlflow.set_experiment(mlflow_experiment)
        # mlflow.autolog()

        # with mlflow.start_run():
        return svc_model_training(dp)
        
        
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='select_best',
        task_id='select_best',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '0.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def select_best_task():
        import sys
        sys.path.insert(1, '/git/TFG_Demo/src/redwine')
        from Deployment.select_best_model import select_best_model
        
        """
        MODIFY WHAT YOU WANT
        """
        
        return select_best_model()
    
    @task.kubernetes(
        image='mfernandezlabastida/kaniko:1.0',
        name='build_inference',
        task_id='build_inference',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '1.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def build_inference_task(run_id):
        import mlflow
        import os
        import logging
        import subprocess

        """
        MODIFY WHAT YOU WANT
        """
        path = '/git/TFG_Demo/build_docker'
        endpoint = 'registry-docker-registry.registry.svc.cluster.local:5001/redwine:prod'


        def download_artifacts(run_id, path):
            mlflow.set_tracking_uri("http://mlflow-tracking.mlflow.svc.cluster.local:5000")

            local_path = mlflow.artifacts.download_artifacts(run_id=run_id, dst_path=path)

            # Buscar el archivo model.pkl y moverlo a la carpeta local_path en caso de que se encuentre en una subcarpeta
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.startswith("model"):
                        logging.info(f"Encontrado archivo model.pkl en: {root}")
                        os.rename(os.path.join(root, file), os.path.join(local_path + '/model', file))
                    elif file.startswith("requirements"):
                        logging.info(f"Encontrado archivo requirements.txt en: {root}")
                        os.rename(os.path.join(root, file), os.path.join(path, file))

        logging.warning(f"Downloading artifacts from run_id: {run_id['best_run']}")
        download_artifacts(run_id['best_run'], path)

        args = [
            "/kaniko/executor",
            f"--dockerfile={path}/Dockerfile",
            f"--context={path}",
            f"--destination={endpoint}",
            f"--cache=false"
        ]
        result = subprocess.run(
            args,
            check=True  # Lanza una excepción si el comando devuelve un código diferente de cero
        )
        logging.warning(f"Kaniko executor finished with return code: {result.returncode}")
        
        
        
        
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='redis_clean',
        task_id='redis_clean',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        env_vars=env_vars
    )
    def redis_clean_task(ids):
        import redis
    
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
    
        redis_client.delete(*ids)
    
    read_df_result = read_df_task()
    process_df_result = process_df_task(read_df_result)
    train_elastic_result = train_elastic_task(process_df_result)
    SVC_train_result = SVC_train_task(process_df_result)
    select_best_result = select_best_task()
    build_inference = build_inference_task(select_best_result)
    redis_task_result = redis_clean_task([read_df_result, process_df_result])
    
    # Define the order of the pipeline
    read_df_result >> process_df_result >> [train_elastic_result, SVC_train_result] >> select_best_result >> build_inference >> redis_task_result
# Call the DAG 
Redwine_production_example()