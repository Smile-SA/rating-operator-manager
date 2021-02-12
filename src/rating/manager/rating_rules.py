from logging import Logger
from typing import Dict

import kopf
import requests

from datetime import datetime as dt

from rating.manager import utils


@kopf.on.create('rating.alterway.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_creation(body: Dict,
                          spec: Dict,
                          logger: Logger,
                          **kwargs: Dict):
    """
    Create and validate the RatingRules through rating-api after creation in kubernetes.

    :body (Dict) A dictionary containing the created kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'rules': spec.get('rules', {}),
        'metrics': spec.get('metrics', {}),
        'timestamp': timestamp
    }
    try:
        utils.post_for_rating_api(endpoint='/ratingrules/add',
                                  payload=data)
    except utils.ConfigurationException as exc:
        logger.error(f'RatingRules {rules_name} is invalid. Reason: {exc}')
    except requests.exceptions.RequestException:
        raise kopf.TemporaryError(f'Request for RatingRules {rules_name} update failed. retrying in 30s', delay=30)
    else:
        logger.info(
            f'RatingRule {rules_name} created, valid from {timestamp}.')


@kopf.on.update('rating.alterway.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_update(body: Dict,
                        spec: Dict,
                        logger: Logger,
                        **kwargs: Dict):
    """
    Update and validate the RatingRules through rating-api after update in kubernetes.

    :body (Dict) A dictionary containing the updated kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'metrics': spec['metrics'],
        'rules': spec['rules'],
        'timestamp': timestamp
    }
    try:
        utils.post_for_rating_api(endpoint='/ratingrules/update',
                                  payload=data)
    except utils.ApiException:
        logger.warning(f'RatingRules {rules_name} does not exist in storage, ignoring.')
    except utils.ConfigurationException as exc:
        logger.error(f'RatingRules {rules_name} is invalid. Reason: {exc}')
    except requests.exceptions.RequestException:
        logger.error(f'Request for RatingRules {rules_name} update failed.')
    else:
        logger.info(f'Rating rules {rules_name} was updated.')


@kopf.on.delete('rating.alterway.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_deletion(body: Dict,
                          spec: Dict,
                          logger: Logger,
                          **kwargs: Dict):
    """
    Delete the RatingRules through rating-api after deletion in kubernetes.

    :body (Dict) A dictionary containing the deleted kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'timestamp': int(dt.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').timestamp())
    }

    try:
        utils.post_for_rating_api(endpoint='/ratingrules/delete',
                                  payload=data)
    except utils.ApiException:
        logger.warning(f'RatingRules {rules_name} does not exist in storage, ignoring.')
    except requests.exceptions.RequestException:
        logger.error(f'Request for RatingRules {rules_name} deletion failed.')
    else:
        logger.info(f'RatingRules {rules_name} ({timestamp}) was deleted.')


@kopf.on.delete('rating.alterway.fr', 'v1', 'ratedmetrics')
@utils.assert_rating_namespace
def delete_rated_metric(body: Dict,
                        spec: Dict,
                        logger: Logger,
                        **kwargs: Dict):
    """
    Delete the data associated with the ratedMetrics through the rating-api.

    :body (Dict) A dictionary containing the deleted kubernetes object.
    :spec (Dict) A smaller version of body.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding unused parameters.
    """
    data = {
        'metric': spec['metric']
    }
    response = utils.post_for_rating_api(endpoint='/rated/frames/delete',
                                         payload=data)
    if response:
        logger.info(f'deleted {response["results"]} rows associated with {body["metadata"]["name"]}')
