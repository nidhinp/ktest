from workflow import main


def test_existing_ns(kube, clusterinfo):
    namespace = 'foobartest123'
    main(namespace)
    main(namespace)

    namespaces = kube.get_namespaces()
    assert namespace in namespaces

    deployments = kube.get_deployments(namespace=namespace)
    assert 'argo-server' in deployments
    assert 'workflow-controller' in deployments

    # teardown
    kube.delete(namespaces.get(namespace))


def test_new_ns(kube, clusterinfo):
    namespace = 'foobartest456'
    main(namespace)

    namespaces = kube.get_namespaces()
    assert namespace in namespaces

    deployments = kube.get_deployments(namespace=namespace)
    assert 'argo-server' in deployments
    assert 'workflow-controller' in deployments

    # teardown
    kube.delete(namespaces.get(namespace))
