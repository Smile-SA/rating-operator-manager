from logging import Logger
from typing import Dict

import kopf
import requests

from datetime import datetime as dt

from rating.manager import utils


@kopf.on.create('rating.smile.fr', 'v1', 'ratingruleinstances')
@kopf.on.update('rating.smile.fr', 'v1', 'ratingruleinstances')
@utils.assert_rating_namespace
def rating_instances_creation_smile(body: Dict,
                                    spec: Dict,
                                    logger: Logger,
                                    **kwargs: Dict):
    handle_rating_instances_creation(body, spec, logger, **kwargs)

def handle_rating_instances_creation(body: Dict,
                                     spec: Dict,
                                     logger: Logger,
                                     **kwargs: Dict):
    """
    Create values of RatingRuleInstances through rating-api after creation in Kubernetes.

    :param body: A dictionary containing the created Kubernetes object.
    :type body: Dict
    :param spec: A smaller version of body.
    :type spec: Dict
    :param logger: A Logger object to log information.
    :type logger: Logger
    :param kwargs: A dictionary holding unused parameters.
    :type kwargs: Dict
    """
    required_keys = ['cpu', 'memory', 'price']
    if all(key in spec for key in required_keys):
        rules_name = body['metadata']['name']
        data = {
            'metric_name': spec.get('name', {}),
            'timeframe': spec.get('timeframe', {}),
            'promql': spec.get('metric', {}),
            'cpu': spec.get('cpu', {}),
            'memory': spec.get('memory', {}),
            'price': spec.get('price', {})
        }
        try:
            utils.post_for_rating_api(endpoint='/templates/metric/add', payload=data)
            utils.post_for_rating_api(endpoint='/templates/instance/add', payload=data)
        except utils.ConfigurationExceptionError as exc:
            logger.error(f'RatingRulesInstance {rules_name} is invalid. Reason: {exc}')
        except requests.exceptions.RequestException:
            logger.error(f'Request for RatingRulesInstance {rules_name} update failed')
        else:
            logger.info(f'RatingRule {rules_name} created/updated.')

@kopf.on.delete('rating.smile.fr', 'v1', 'ratingruleinstances')
@utils.assert_rating_namespace
def rating_instances_deletion_smile(body: Dict,
                                    spec: Dict,
                                    logger: Logger,
                                    **kwargs: Dict):
    handle_rating_instances_deletion(body, spec, logger, **kwargs)

def handle_rating_instances_deletion(body: Dict,
                                     spec: Dict,
                                     logger: Logger,
                                     **kwargs: Dict):
    """
    Delete values of RatingRuleInstances through rating-api after deletion in Kubernetes.

    :param body: A dictionary containing the deleted Kubernetes object.
    :type body: Dict
    :param spec: A smaller version of body.
    :type spec: Dict
    :param logger: A Logger object to log information.
    :type logger: Logger
    :param kwargs: A dictionary holding unused parameters.
    :type kwargs: Dict
    """
    required_keys = ['cpu', 'memory', 'price']
    if all(key in spec for key in required_keys):
        rules_name = body['metadata']['name']
        data = {
            'metric_name': spec.get('name', {}),
        }
        try:
            utils.post_for_rating_api(endpoint='/templates/metric/delete', payload=data)
            utils.post_for_rating_api(endpoint='/templates/instance/delete', payload=data)
        except utils.ConfigurationExceptionError as exc:
            logger.error(f'RatingRulesInstance {rules_name} is invalid. Reason: {exc}')
        except requests.exceptions.RequestException:
            logger.error(f'Request for RatingRulesInstance {rules_name} delete failed')
        else:
            logger.info(f'RatingRule {rules_name} deleted.')
