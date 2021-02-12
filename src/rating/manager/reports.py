from logging import Logger
from typing import AnyStr, Dict
import kopf

from datetime import datetime as dt

from rating.manager import utils
from rating.manager import metrics
from rating.manager import rules
from rating.manager import rated_metrics
from rating.manager.bisect import get_closest_configs_bisect


def retrieve_configurations_from_API() -> Dict:
    """Wrap the configuration retrieval from the rating-api."""
    return utils.get_from_rating_api(endpoint='/ratingrules/list/local')


def retrieve_last_rated_report(report_name: AnyStr) -> AnyStr or None:
    """Get the timestamp of the last rating time, for a given report."""
    results = utils.get_from_rating_api(endpoint=f'/reports/{report_name}/last_rated')
    if results:
        return results[0]['last_insert']
    return None


def rated_or_not(report_name: AnyStr) -> dt:
    """Get a timestamp corresponding to the last rated frame for a report, or 0."""
    timestamp = retrieve_last_rated_report(report_name)
    if timestamp:
        # [:-4] because of comma and milliseconds
        return dt.strptime(timestamp[:-4], '%a, %d %b %Y %H:%M:%S')
    return dt.utcfromtimestamp(0)


def select_end_period(valid_from: dt,
                      valid_to: dt) -> dt:
    """
    Return the "end" variable for a query, according to inputs.

    :valid_from (datetime) Start date for the query.
    :valid_to (datetime) End date for the query.

    Return either a timestamp representing now, or end.
    """
    if valid_to == valid_from or \
       valid_to == dt(2100, 1, 1, 1, 1).strftime('%s'):  # dt(2100, 1, 1, 1, 1) represent an arbitrary max date for the rating.
        return dt.utcnow()
    return dt.utcfromtimestamp(int(valid_to))


def extract_metric_config(source: Dict,
                          target: AnyStr,
                          match: AnyStr) -> AnyStr or None:
    """
    Extract the matching configuration from source.

    :source (Dict) A dictionary holding the configuration.
    :target (AnyStr) A string representing the key to match.
    :match (AnyStr) A string representing the value to find.

    Return none or the matched value.
    """
    for key in source.keys():
        if source[key][target] == match:
            source[key]['metric'] = key
            return source[key]
    return None


def check_rating_conditions(report_name: AnyStr,
                            table_name: AnyStr,
                            begin: dt,
                            configuration: Dict) -> Dict:
    """
    Check and fill the configuration for the rating.

    :report_name (AnyStr) The name of the report to be rated.
    :table_name (AnyStr) The name of the table to use to get data.
    :begin (datetime) The timestamp from which to recover the frames from table_name.
    :configuration (Dict) The configuration to use to rate the frames.

    Return the full configuration to run the rating mechanism.
    """
    metric_config = extract_metric_config(
        metrics.ensure_metrics_config(configuration['metrics']['metrics']),
        'report_name',
        report_name)
    if not metric_config:
        return {}

    rating_config = metric_config
    rating_config.update({
        'presto_table': table_name.replace('-', '_'),
        'begin': begin,
        'end': select_end_period(configuration['valid_from'],
                                 configuration['valid_to'])
    })
    return rating_config


@kopf.on.event('metering.openshift.io', 'v1', 'reports')
def report_event(body: Dict,
                 logger: Logger,
                 **kwargs: Dict):
    """
    Catch events of reports and rate the frames.

    :body (Dict) A dictionary containing the report object.
    :logger (Logger) A Logger object to log informations.
    :kwargs (Dict) A dictionary holding optional parameters.
    """
    metadata = body['metadata']
    if kwargs["type"] not in ['ADDED', 'MODIFIED']:
        return

    configurations = retrieve_configurations_from_API()
    if not configurations:
        raise utils.ConfigurationMissing(
            'Bad response from API, no configuration found.'
        )
    begin = rated_or_not(metadata['name'])
    configs = tuple(ts['valid_from'] for ts in configurations)
    choosen_config = get_closest_configs_bisect(
        begin.strftime('%s'),
        configs)

    table = kwargs['status'].get('tableRef')
    if not table:
        return
    metric_config = check_rating_conditions(metadata['name'],
                                            table['name'],
                                            begin,
                                            configurations[choosen_config])
    if not metric_config:
        return
    logger.info(f'using config with timestamp {configs[choosen_config]}')
    logger.info(
        'rating for {metric} in {table} for period {begin} to {end} started..'
        .format(metric=metric_config['metric'],
                table=metric_config['presto_table'],
                begin=metric_config['begin'],
                end=metric_config['end'])
    )
    rules.ensure_rules_config(configurations[choosen_config]['rules']['rules'])
    rated_metrics.retrieve_data(
        configurations[choosen_config]['rules']['rules'],
        metric_config,
        logger)
