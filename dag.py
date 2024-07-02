from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.models import Variable

@dag(
    description='Demo',
    schedule_interval='* 12 * * *', 
    start_date=datetime(2024, 7, 2),
    catchup=False,
    tags=['demo', 'TFG'],
)
def TFG_Demo_dag():

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
        "MLFLOW_TRACKING_PASSWORD": Variable.get("MLFLOW_TRACKING_PASSWORD")
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
        image='mfernandezlabastida/gpu_test:0.2',
        image_pull_policy='Always',
        name='get_dataframe',
        task_id='get_dataframe',
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
    def get_dataframe_task():
        import sys
        import redis
        import uuid
        import pickle
    
        sys.path.insert(1, '/git/TFG_Demo/')
        from read import get_dataset
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
    
        df = get_dataset()
        
        df_id = str(uuid.uuid4())
        redis_client.set(df_id, pickle.dumps(df))
    
        return df_id
    
    @task.kubernetes(
        image='mfernandezlabastida/gpu_test:0.2',
        image_pull_policy='Always',
        name='train_GPU',
        task_id='train_GPU',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        node_selector={"kubernetes.io/hostname": "master"},
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5', 'nvidia.com/gpu': '1'},
            limits={'cpu': '0.5', 'nvidia.com/gpu': '1'}
        ),
        full_pod_spec=pod_spec,
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def train_GPU_task(df_id):
        import sys
        import redis
        import uuid
        import pickle
    
        sys.path.insert(1, '/git/TFG_Demo/')
        from train import train_and_evaluate
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
        df = pickle.loads(redis_client.get(df_id))
    
        return train_and_evaluate(df)
        
        
    @task.kubernetes(
        image='mfernandezlabastida/gpu_test:0.2',
        image_pull_policy='Always',
        name='train_CPU',
        task_id='train_CPU',
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
    def train_CPU_task(df_id):
        import sys
        import redis
        import uuid
        import pickle
    
        sys.path.insert(1, '/git/TFG_Demo/')
        from train import train_and_evaluate
        
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )
        df = pickle.loads(redis_client.get(df_id))
    
        return train_and_evaluate(df)
        
        
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        image_pull_policy='Always',
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
    
    get_dataframe_result = get_dataframe_task()
    train_GPU_result = train_GPU_task(get_dataframe_result)
    train_CPU_result = train_CPU_task(get_dataframe_result)
    redis_task_result = redis_clean_task([get_dataframe_result])
    
    # Define the order of the pipeline
    get_dataframe_result >> [train_GPU_result, train_CPU_result] >> redis_task_result
# Call the DAG 
TFG_Demo_dag()