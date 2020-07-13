import os
import yaml

from subprocess import call

import kubernetes.client
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException
from kubernetes.utils import create_from_yaml, FailToCreateError
from kubernetes.config import load_kube_config, load_incluster_config


def create_ns(ns):
    try:
        load_kube_config()
    except TypeError:
        load_incluster_config()

    api_instance = CoreV1Api()
    body = kubernetes.client.V1Namespace(
        metadata=kubernetes.client.V1ObjectMeta(name=ns))

    try:
        api_response = api_instance.create_namespace(body)
        if api_response.to_dict().get('metadata').get('name') == ns:
            install_argo(ns)
            install_data_bridge(ns)
            run_argo_workflow(ns)
        else:
            print('Unable to create ns')
    except ApiException as e:
        run_argo_workflow(ns) if e.status == 409 else print(e)


def install_argo(ns):
    client = kubernetes.client.ApiClient()
    outfile_path = 'rb.yml'
    with open('rolebinding.yml') as rfp:
        with open(outfile_path, 'w') as wfp:
            manifest = yaml.safe_load(rfp)
            manifest['metadata']['namespace'] = ns
            for data in manifest.get('subjects'):
                data["namespace"] = ns
            yaml.dump(manifest, wfp)
    try:
        create_from_yaml(client, 'quick-start.yml', namespace=ns)
        create_from_yaml(client, outfile_path, namespace=ns)
    except FailToCreateError:
        pass
    os.remove(outfile_path)


def run_argo_workflow(ns):
    call([
        'argo', 'submit', '-n', ns,
        'https://raw.githubusercontent.com/argoproj/argo/master/examples/artifact-passing.yaml'
    ])


def install_data_bridge(ns):
    client = kubernetes.client.ApiClient()
    outfile_path = 'data-bridge/rolebinding.yml'
    with open('data-bridge/clusterrolebinding.yml') as rfp:
        with open(outfile_path, 'w') as wfp:
            manifest = yaml.safe_load(rfp)
            for data in manifest.get('subjects'):
                data["namespace"] = ns
            yaml.dump(manifest, wfp)
    try:
        create_from_yaml(client, 'data-bridge/sa.yml', namespace=ns)
        create_from_yaml(client, 'data-bridge/clusterrole.yml', namespace=ns)
        create_from_yaml(client, outfile_path, namespace=ns)
    except FailToCreateError:
        pass
    os.remove(outfile_path)


def main(namespace):
    create_ns(namespace)


if __name__ == '__main__':
    namespace = 'foo'
    main(namespace)
