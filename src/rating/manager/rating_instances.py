from logging import Logger
from typing import Dict

import kopf
import requests

from datetime import datetime as dt

from rating.manager import utils

@kopf.on.create('rating.alterway.fr', 'v1', 'ratingruleinstances')
@kopf.on.update('rating.alterway.fr', 'v1', 'ratingruleinstances')
@utils.assert_rating_namespace
def rating_instances_creation(body: Dict,
                              spec: Dict,
                              logger: Logger,
                              **kwargs: Dict):
    """
    Create values of RatingRuleInstances through rating-api after creation in kubernetes.

    :body (Dict) A dictionary containing the created kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    if 'cpu' in spec.keys() and 'memory' in spec.keys() and 'price' in spec.keys():
        rules_name = body['metadata']['name']
        data = {
            'metric_name': spec.get('name', {}),
            'timeframe': spec.get('timeframe', {}),
            'cpu': spec.get('cpu', {}),
            'memory': spec.get('memory', {}),
            'price': spec.get('price', {}),
        }
        try:
            utils.post_for_rating_api(endpoint='/templates/metric/add',
                                      payload=data)
        except utils.ConfigurationExceptionError as exc:
            logger.error(f'RatingRulesInstance {rules_name} is invalid. Reason: {exc}')
        except requests.exceptions.RequestException:
            logger.error(f'Request for RatingRulesInstance {rules_name} update failed')
        else:
            logger.info(
                f'RatingRule {rules_name} created/updated.')


@kopf.on.delete('rating.alterway.fr', 'v1', 'ratingruleinstances')
@utils.assert_rating_namespace
def rating_instances_deletion(body: Dict,
                              spec: Dict,
                              logger: Logger,
                              **kwargs: Dict):
    """
    Delete values of RatingRuleInstances through rating-api after creation in kubernetes.

    :body (Dict) A dictionary containing the deleted kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    if 'cpu' in spec.keys() and 'memory' in spec.keys() and 'price' in spec.keys():
        rules_name = body['metadata']['name']
        data = {
            'metric_name': spec.get('name', {}),
        }
        try:
            utils.post_for_rating_api(endpoint='/templates/metric/delete',
                                      payload=data)
        except utils.ConfigurationExceptionError as exc:
            logger.error(f'RatingRulesInstance {rules_name} is invalid. Reason: {exc}')
        except requests.exceptions.RequestException:
            logger.error(f'Request for RatingRulesInstance {rules_name} delete failed')
        else:
            logger.info(
                f'RatingRule {rules_name} deleted.')