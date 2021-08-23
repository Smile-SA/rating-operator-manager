from typing import Dict
import kopf
from kubernetes import client, config
from base64 import b64decode
import os
from kubernetes.client.rest import ApiExceptionError

from rating.manager import utils
from rating.manager import rating_rules


def register_admin_key(api: client.CoreV1Api):
    """
    Register the admin key from the administrator secret.

    :api (client.CoreV1Api) The api client to use to execute the request.
    """
    namespace = utils.envvar('RATING_NAMESPACE')
    secret_name = f'{namespace}-admin'
    try:
        secret_encoded_bytes = api.read_namespaced_secret(secret_name, namespace).data
    except ApiExceptionError as exc:
        raise exc
    rating_admin_api_key = list(secret_encoded_bytes.keys())[0]
    os.environ[rating_admin_api_key] = b64decode(
        secret_encoded_bytes[rating_admin_api_key]).decode('utf-8')


@kopf.on.create('', 'v1', 'namespaces')
@kopf.on.update('', 'v1', 'namespaces')
def callback_namespace_tenant(body: Dict, **kwargs: Dict):
    """
    Update a namespace after a create or update event.

    :body (Dict) A dictionary representing the kubernetes object affected by the event.
    :kwargs (Dict) A dictionary containing optional parameters (for compatibility).
    """
    update_namespace_tenant(body['metadata'])


def update_namespace_tenant(metadata: Dict):
    """
    Update the tenant of a namespace through the rating-api.

    :metadata (Dict) A dictionary containing the metadata values of the object.
    """
    tenant = None
    tenants = []

    annotations = metadata.get('annotations')
    if annotations:
        tenant = annotations.get('openshift.io/requester')

    labels = metadata.get('labels')
    if labels:
        tenants = (labels.get('tenants', "")).split('-')
        if not tenant:
            tenants.append(labels.get('tenant'))
    else:
        tenants = ['']

    for tenant in tenants:
        payload = {
            'tenant_id': tenant or 'default',
            'namespace': metadata['name']
        }
        utils.post_for_rating_api(endpoint='/namespaces/tenant', payload=payload)


def scan_cluster_namespaces(api: client.CoreV1Api):
    """
    Scan the namespaces in the cluster and attribute them tenant_id.

    If no annotation or label named 'tenant' exist, tenant will be default.

    :api (client.CoreV1Api) The api client to use to execute the request.
    """
    try:
        namespace_list = api.list_namespace()
    except ApiExceptionError as exc:
        raise exc
    for namespace_obj in namespace_list.items:
        update_namespace_tenant(
            namespace_obj.to_dict()['metadata']
        )

@kopf.on.startup()
def callback_startup(**kwargs: Dict):
    """
    Execute the startup routine, registering administrator key and namespaces tenants.

    :kwargs (Dict) A dictionary containing optional parameters (for compatibility).
    """
    metering = os.environ.get('METERING_OPERATOR')
    if metering:
        from rating.manager import reports
    config.load_incluster_config()
    api = client.CoreV1Api()
    register_admin_key(api)
    kwargs['logger'].info('Registered admin token.')
    scan_cluster_namespaces(api)
    kwargs['logger'].info('Registered active namespaces.')
    update_namespace_tenant({'name': 'unspecified'})


@kopf.on.login()
def callback_login(**kwargs: Dict) -> kopf.ConnectionInfo:
    """
    Execute the login routine, authenticating the client if needed.

    :kwargs (Dict) A dictionary containing optional parameters (for compatibility).
    """
    if utils.envvar_bool('AUTH'):
        return kopf.ConnectionInfo(
            server=os.environ.get('KUBERNETES_PORT').replace('tcp', 'https'),
            ca_path='/var/run/secrets/kubernetes.io/serviceaccount/ca.crt',
            scheme='Bearer',
            token=open("/var/run/secrets/kubernetes.io/serviceaccount/token", "r").read()
        )
    # Black magic here, don't ask why the second does not work
    # Or look it out yourself, but be aware that you might encounter elves and dragons along the way...
    return kopf.login_via_client(**kwargs)
    # return kopf.login_via_pykube(**kwargs)
