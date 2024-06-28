from logging import Logger
from typing import Dict

import kopf
import requests

from datetime import datetime as dt

from rating.manager import utils



@kopf.on.create('rating.smile.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_creation_smile(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    handle_rating_rules_creation(body, spec, logger, **kwargs)

def handle_rating_rules_creation(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'rules': spec.get('rules', {}),
        'metrics': spec.get('metrics', {}),
        'timestamp': timestamp
    }
    try:
        utils.post_for_rating_api(endpoint='/ratingrules/add', payload=data)
    except utils.ConfigurationExceptionError as exc:
        logger.error(f'RatingRules {rules_name} is invalid. Reason: {exc}')
    except requests.exceptions.RequestException:
        raise kopf.TemporaryError(f'Request for RatingRules {rules_name} update failed. retrying in 30s', delay=30)
    else:
        logger.info(f'RatingRule {rules_name} created, valid from {timestamp}.')



@kopf.on.update('rating.smile.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_update_smile(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    handle_rating_rules_update(body, spec, logger, **kwargs)

def handle_rating_rules_update(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'metrics': spec['metrics'],
        'rules': spec['rules'],
        'timestamp': timestamp
    }
    try:
        utils.post_for_rating_api(endpoint='/ratingrules/update', payload=data)
    except utils.ApiExceptionError:
        logger.warning(f'RatingRules {rules_name} does not exist in storage, ignoring.')
    except utils.ConfigurationExceptionError as exc:
        logger.error(f'RatingRules {rules_name} is invalid. Reason: {exc}')
    except requests.exceptions.RequestException:
        logger.error(f'Request for RatingRules {rules_name} update failed.')
    else:
        logger.info(f'Rating rules {rules_name} was updated.')



@kopf.on.delete('rating.smile.fr', 'v1', 'ratingrules')
@utils.assert_rating_namespace
def rating_rules_deletion_smile(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    handle_rating_rules_deletion(body, spec, logger, **kwargs)

def handle_rating_rules_deletion(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    timestamp = body['metadata']['creationTimestamp']
    rules_name = body['metadata']['name']
    data = {
        'timestamp': int(dt.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').timestamp())
    }
    try:
        utils.post_for_rating_api(endpoint='/ratingrules/delete', payload=data)
    except utils.ApiExceptionError:
        logger.warning(f'RatingRules {rules_name} does not exist in storage, ignoring.')
    except requests.exceptions.RequestException:
        logger.error(f'Request for RatingRules {rules_name} deletion failed.')
    else:
        logger.info(f'RatingRules {rules_name} ({timestamp}) was deleted.')

@kopf.on.delete('rating.smile.fr', 'v1', 'ratedmetrics')
@utils.assert_rating_namespace
def delete_rated_metric_smile(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    handle_delete_rated_metric(body, spec, logger, **kwargs)



def handle_delete_rated_metric(body: Dict, spec: Dict, logger: Logger, **kwargs: Dict):
    data = {
        'metric': spec['metric']
    }
    response = utils.post_for_rating_api(endpoint='/rated/frames/delete', payload=data)
    if response:
        logger.info(f'deleted {response["results"]} rows associated with {body["metadata"]["name"]}')
