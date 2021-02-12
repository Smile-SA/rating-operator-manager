from logging import Logger
from typing import AnyStr, Dict, List, Tuple
from datetime import datetime as dt

from rating.manager import utils
from rating.manager import rates
from rating.manager import rules as rs


def get_labels_from_table(table: AnyStr, column_name: AnyStr) -> List[AnyStr]:
    """
    Get a list of labels from a given presto table.

    :table (AnyStr) A string representing the table.
    :column_name (AnyStr) A string representing the column_name to look for.

    Return a list of labels.
    """
    columns = utils.get_from_rating_api(endpoint=f'/presto/{table}/columns')
    return [col['column_name'] for col in columns if col['column_name'] not in [
        'period_start',
        'period_end',
        'pod',
        'namespace',
        'node',
        column_name]
    ]


def extract_frames_labels(frame: Dict,
                          column_name: AnyStr,
                          labels: List) -> Dict:
    """
    Extract the labels from a frame.

    :frame (Dict) A dictionary containing the frame.
    :column_Name (AnyStr) A string representing the name of the column.
    :labels (List) A list containing the labels names.

    Return a dictionary containing the frame labels.
    """
    frame_labels = {}
    for key, value in frame.items():
        if key not in ['period_start',
                       'period_end',
                       'pod',
                       'namespace',
                       'node',
                       column_name] and key in labels:
            frame_labels[key] = value
    return frame_labels


def get_frames(metric_config: Dict, labels: Dict) -> Dict:
    """
    Get frames from the rating-api.

    :metric_config (Dict) A dictionary containing the metric configuration.
    :labels (Dict) A dictionary holding the labels key:value

    Return the response of the rating-api, as a dictionary.
    """
    payload = {
        'labels': labels,
        'column': metric_config['presto_column'],
        'start': metric_config['begin'].isoformat(sep=' ', timespec='milliseconds'),
        'end': metric_config['end'].isoformat(sep=' ', timespec='milliseconds')
    }
    return utils.get_from_rating_api(
        endpoint=f'/presto/{metric_config["presto_table"]}/frames',
        payload=payload)


def update_rated_data(rated_frames: List[Tuple],
                      rated_namespaces: List,
                      metric_config: Dict,
                      timestamp: dt) -> Dict:
    """
    Update the rated data with new frames.

    :rated_frames (List[Tuple]) A list of tuple containing the frames to insert.
    :rated_namespaces (List) A list containing the namespaces concerned by the rating.
    :metric_config (Dict) A dictionary holding the configuration for the current metric.
    :timestamp (datetime) A timestamp representing the time of rating.

    Return the response of the rating-api, as a dictionary.
    """
    payload = {
        'rated_frames': rated_frames,
        'rated_namespaces': rated_namespaces,
        'report_name': metric_config['report_name'],
        'metric': metric_config['metric'],
        'last_insert': timestamp
    }
    return utils.post_for_rating_api(endpoint='/rated/frames/add',
                                     payload=payload)

def retrieve_data(rules: Dict,
                  metric_config: Dict,
                  logger: Logger):
    """
    Retrieve and rate data according to rules and metrics configuration.

    :rules (Dict) A dictionary holding the rules to rate the frames.
    :metric_config (Dict) A dictionary holding the metrics configuration.
    """
    logger.info(f'Loading frames from {metric_config["presto_table"]}..')
    logger.info('checking for labels..')
    labels_name = get_labels_from_table(metric_config['presto_table'],
                                        metric_config['presto_column'])

    potential_labels = ', '.join(labels_name)
    if len(potential_labels) > 0:
        logger.info(f'found labels: {potential_labels}')
        potential_labels = f', {potential_labels}'
    else:
        logger.info('no labels found')

    frames = get_frames(metric_config, potential_labels)
    loaded = len(frames)
    if loaded == 0:
        logger.info('no frames loaded')
        return
    logger.info(f'{loaded} frames loaded')

    rated_frames, rated_namespaces = [], []
    rating_time = dt.utcnow()
    for frame in frames:
        # 6 here because every columns after is considered a label
        frame_labels = extract_frames_labels(frame,
                                             metric_config['presto_column'],
                                             labels_name)
        labels, rule = rs.find_match(metric_config['metric'],
                                     frame_labels,
                                     rules)
        converted = rates.convert_metrics_unit(
            metric_config['unit'],
            rule['unit'],
            frame[metric_config['presto_column']]
        )

        rated_frames.append((
            frame['period_start'],                              # frame_begin
            frame['period_end'],                                # frame_end
            frame['namespace'],                                 # namespace
            frame['node'],                                      # node
            metric_config['metric'],                            # metric
            frame['pod'],                                       # pod
            converted,                                          # quantity
            rates.rate(rule, {'qty': converted}),               # rating
            f'{labels}'
        ))
        rated_namespace = frame['namespace']
        if rated_namespace not in rated_namespaces:
            rated_namespaces.append(rated_namespace)
    logger.info('frame processed')

    logger.info('sending data..')
    result = update_rated_data(rated_frames,
                               rated_namespaces,
                               metric_config,
                               rating_time.isoformat(sep=' ', timespec='milliseconds'))
    if result:
        logger.info(f'updated rated-{metric_config["metric"].replace("_", "-")} object')
    logger.info('finished rating instance')
