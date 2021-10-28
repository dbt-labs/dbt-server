from kubernetes import client, config
from dbt.logger import log_manager, GLOBAL_LOGGER as logger
import os


def delete_server_deployment(deployment_name):
    apps_v1_api = None

    try:
        config.load_incluster_config()
        apps_v1_api = client.AppsV1Api()

    except Exception as e:
        logger.warning(f"Failed to initialize kubernetes client. Error {e}")

    pod_namespace = os.getenv("POD_NAMESPACE")
    result = None
    try:
        result = apps_v1_api.delete_namespaced_deployment(
            name=deployment_name,
            namespace=pod_namespace,
            body=client.V1DeleteOptions(
                propagation_policy="Foreground"
            ),
            _request_timeout=300,
        )
    except client.rest.ApiException:
        pass
    return result
