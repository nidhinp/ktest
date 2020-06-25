from subprocess import call

import kubernetes.client
from kubernetes.config import load_kube_config, load_incluster_config
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException


def creaet_ns(ns):
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
            run_argo_workflow(ns)
        else:
            print('Unable to create ns')
    except ApiException as e:
        run_argo_workflow(ns) if e.status == 409 else print(e)


def install_argo(ns):
    call(['kubectl', 'apply', '-n', ns, '-f', 'quick-start.yml'])
    call([
        'kubectl', 'create', 'rolebinding', 'default-admin',
        '--clusterrole=admin', f'--serviceaccount={ns}:default', '-n', ns
    ])


def run_argo_workflow(ns):
    call([
        'argo', 'submit', '-n', ns,
        'https://raw.githubusercontent.com/argoproj/argo/master/examples/artifact-passing.yaml'
    ])


def main():
    namespace = 'foo'
    creaet_ns(namespace)


if __name__ == '__main__':
    main()
