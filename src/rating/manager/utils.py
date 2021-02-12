from typing import AnyStr, Callable, Dict
import kopf
import logging
import os
import re
import requests
import sys


class ConfigurationMissing(Exception):
    """Simple error class to handle missing configuration api side."""

    pass


class ConfigurationException(Exception):
    """Simple error class to handle rating configuration errors."""

    pass


class ApiException(Exception):
    """Simple error class to handle rating-api related errors."""

    pass


def assert_rating_namespace(func: Callable) -> Callable:
    """
    Assert that the namespace of the requests is covered by the rating-operator.

    :func (Callable) The decorated function.

    Return the wrapped function.
    """
    def wrapper(**kwargs: Dict) -> Callable:
        """
        Assert that the namespace of the requests is covered by the rating-operator.

        :kwargs (Dict) A dictionary containing all the parameter for the callback.

        Return the wrapped function.
        """
        namespace = kwargs['body']['metadata']['namespace']
        rating_namespace = envvar('RATING_NAMESPACE')
        if namespace != rating_namespace:
            kwargs['logger'].info(f'event not in {rating_namespace} namespace, discarding')
            return {}
        return func(**kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


def admin_token(func: Callable) -> Callable:
    """
    Inject the admin token in the payload of the request.

    :func (Callable) The decorated function.

    Return the wrapped function.
    """
    def wrapper(**kwargs: Dict) -> Callable:
        """
        Inject the admin token in the payload of the request.

        :kwargs (Dict) A dictionary containing all the parameter for the callback.

        Return the wrapped function.
        """
        payload = kwargs.get('payload', {})
        payload.update({
            'token': envvar('RATING_ADMIN_API_KEY')
        })
        kwargs['payload'] = payload
        return func(**kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


@admin_token
def get_from_rating_api(endpoint: AnyStr, payload: Dict) -> Dict:
    """
    Send a GET request to the given endpoint of the rating-api.

    :endpoint (AnyStr) The endpoint to which to send the request.
    :payload (Dict) A dictionary containing everything to be embedded in the request.

    Return the results of the requests, as a dictionary.
    """
    api_url = envvar('RATING_API_URL')
    response = requests.get(f'{api_url}{endpoint}', params=payload)
    try:
        response.raise_for_status()
    except requests.exceptions.RequestException:
        raise kopf.TemporaryError('rated data failed to be retrieved, retrying in 5s..', delay=5)
    content = response.json()
    return content.get('results', {})


@admin_token
def post_for_rating_api(endpoint: AnyStr, payload: Dict) -> Dict:
    """
    Send a POST request to the given endpoint of the rating-api.

    :endpoint (AnyStr) The endpoint to which to send the request.
    :payload (Dict) A dictionary containing everything to be embedded in the request.

    Return the results of the requests, as a dictionary.
    """
    api_url = envvar('RATING_API_URL')
    headers = {
        'content-type': 'application/json'
    }
    response = requests.post(url=f'{api_url}{endpoint}', headers=headers,json=payload)
    if response.status_code == 400:  # When ratingrule is wrong
        raise ConfigurationException(response.content.decode("utf-8"))
    elif response.status_code == 404:  # When object is not found
        raise ApiException
    try:
        response.raise_for_status()
    except requests.exceptions.RequestException:
        raise kopf.TemporaryError('rated data failed to be transmitted (connection error), retrying in 5s..', delay=5)
    return response.json()


def is_valid_against(target: AnyStr, regexp: AnyStr) -> bool:
    """
    Validate a string against a given regular exepression.

    :target (AnyStr) A string representing the target to be validated.
    :regexp (AnyStr) A string representing the regular expression to be matched against.

    Return a boolean reflecting the result of the match.
    """
    return re.match(regexp, target) is not None


def envvar_bool(name: AnyStr) -> bool:
    """
    Return a boolean value of the variable.

    This function exist because some variables might be 'true' or 'false', as strings.

    :name (AnyStr) The name of the environment variable.

    Return a boolean corresponding to the name.
    """
    value = os.environ.get(name, 'false').lower()
    if value == 'false':
        return False
    return True


def envvar(name: AnyStr) -> AnyStr:
    """Return the value of an environment variable, or die trying."""
    try:
        return os.environ[name]
    except KeyError:
        logging.error('Missing envvar $%s', name)
        sys.exit(1)
