import re
import yaml
import logging

import kubernetes.client


def describe(obj):
    return "%s '%s'" % (obj['kind'], obj['metadata']['name'])


def camel2snake(string):
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
    return string


def invoke_api(k8sApi, namespace, action, obj, **args):
    ''' find a suitalbe function and perform the actual API invocation.
        @param k8sApi: client object for the invocation, wired to correct
        API version
        @param action: either 'create' (to inject a new objet) or 'replace',
        'patch','delete'
        @param obj: the full object spec to be passed into the API invocation
        @param args: (optional) extraneous arguments to pass
        @return: response object from Kubernetes API call
    '''
    # transform ActionType from Yaml into action_type for swagger API
    kind = camel2snake(obj['kind'])
    # determine namespace to place the object in, supply default

    functionName = '%s_%s' % (action, kind)
    if hasattr(k8sApi, functionName):
        # namespace agnostic API
        function = getattr(k8sApi, functionName)
    else:
        functionName = '%s_namespaced_%s' % (action, kind)
        function = getattr(k8sApi, functionName)
        args['namespace'] = namespace
    if 'create' not in functionName:
        args['name'] = obj['metadata']['name']
    if 'delete' in functionName:
        from kubernetes.client.models.v1_delete_options import V1DeleteOptions
        obj = V1DeleteOptions()

    return function(body=obj, **args)


def find_K8s_api(obj, client=None):
    ''' Investigate the object spec and lookup the corresponding API object
        @param client: (optional) preconfigured client environment to use
        for invocation
        @return: a client instance wired to the apriopriate API
    '''
    grp, _, ver = obj['apiVersion'].partition('/')
    if ver == '':
        ver = grp
        grp = 'core'
    # Strip 'k8s.io', camel-case-join dot separated parts.
    # rbac.authorization.k8s.io -> RbacAuthorzation
    grp = ''.join(part.capitalize()
                  for part in grp.rsplit('.k8s.io', 1)[0].split('.'))
    ver = ver.capitalize()

    k8sApi = '%s%sApi' % (grp, ver)
    return getattr(kubernetes.client, k8sApi)(client)


def delete_object(obj, client=None, **kwargs):
    k8sApi = find_K8s_api(obj, client)
    try:
        res = invoke_api(k8sApi, 'delete', obj, **kwargs)
        logging.debug('K8s: %s DELETED. uid was: %s', describe(
            obj), res.details and res.details.uid or '?')
        return True
    except kubernetes.client.rest.ApiException as apiEx:
        if apiEx.reason == 'Not Found':
            logging.warning('K8s: %s does not exist (anymore).', describe(obj))
            return False
        else:
            message = 'K8s: deleting %s FAILED. Exception: %s' % (
                describe(obj), apiEx)
            logging.error(message)
            raise RuntimeError(message)


def patch_object(obj, client=None, **kwargs):
    k8sApi = find_K8s_api(obj, client)
    try:
        res = invoke_api(k8sApi, 'patch', obj, **kwargs)
        logging.debug('K8s: %s PATCHED -> uid=%s',
                      describe(obj), res.metadata.uid)
        return res
    except kubernetes.client.rest.ApiException as apiEx:
        if apiEx.reason == 'Unprocessable Entity':
            message = 'K8s: patch for %s rejected. Exception: %s' % (
                describe(obj), apiEx)
            logging.error(message)
            raise RuntimeError(message)
        else:
            raise


def create_or_update_or_replace(obj, namespace, client=None, **kwargs):
    ''' invoke the K8s API to create or replace a kubernetes object.
        The first attempt is to create(insert) this object; when
        this is rejected because of an existing object with same name, we
        attempt to patch this existing object. As a last resort, if even the
        patch is rejected, we *delete* the existing object and recreate
        from scratch.
        @param obj: complete object spec, including API version and metadata.
        @param client: (optional) preconfigured client environment to use
        for invocation
        @param kwargs: (optional) further args to pass to the call
        @return: response object from Kubernetes API call
    '''
    k8sApi = find_K8s_api(obj, client)
    try:
        res = invoke_api(k8sApi, namespace, 'create', obj, **kwargs)
        logging.debug(
            'K8s: %s created -> uid=%s', describe(obj), res.metadata.uid)
    except kubernetes.client.rest.ApiException as apiEx:
        if apiEx.reason != 'Conflict':
            raise
        try:
            # asking for forgiveness...
            res = invoke_api(k8sApi, namespace, 'patch', obj, **kwargs)
            logging.debug(
                'K8s: %s PATCHED -> uid=%s', describe(obj), res.metadata.uid)
        except kubernetes.client.rest.ApiException as apiEx:
            if apiEx.reason != 'Unprocessable Entity':
                raise
            try:
                # second attempt... delete the existing object and re-insert
                first_str = 'K8s: replacing %s FAILED'
                second_str = 'Attempting deletion and recreation...'
                logging.debug(first_str+second_str, describe(obj))
                res = invoke_api(k8sApi, namespace, 'delete', obj, **kwargs)
                logging.debug('K8s: %s DELETED...', describe(obj))
                res = invoke_api(k8sApi, namespace, 'create', obj, **kwargs)
                logging.debug(
                    'K8s:%sCREATED -> uid=%s', describe(obj), res.metadata.uid)
            except Exception as ex:
                message = 'K8s: FAILURE updating %s. Exception: %s' % (
                    describe(obj), ex)
                logging.error(message)
                raise RuntimeError(message)
    return res


def apply(path, namespace):
    with open(path) as f:
        manifest = yaml.safe_load(f)
        create_or_update_or_replace(manifest, namespace)
