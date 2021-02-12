from typing import Dict
from rating.manager import utils


def ensure_metrics_config(config: Dict) -> Dict:
    """
    Validate that configuration parameters are correct.

    :config (Dict) A dictionary containing the configuration.
    """
    metrics = []
    for metric, conf in config.items():
        if metric in metrics:
            raise utils.ConfigurationException(
                'Duplicated key in metrics definition', metric)
        metrics.append(metric)

        keys = set(conf.keys())
        if len(keys) < 4:
            raise utils.ConfigurationException(
                'Missing key in metrics definition', keys
            )

        accepted_keys = {'report_name', 'presto_table', 'presto_column', 'unit'}
        if keys != accepted_keys:
            raise utils.ConfigurationException(
                'Unsupported key in metrics definition', keys
            )

        for key in conf.values():
            if not utils.is_valid_against(key, '^[a-zA-Z0-9-_]+$'):
                raise utils.ConfigurationException(
                    'Invalid value in metrics definition',
                    key
                )

        unit = conf['unit']
        if unit not in ['core-seconds', 'byte-seconds', 'byte']:
            raise utils.ConfigurationException(
                'Unsupported unit in metrics definition',
                unit
            )
    return config
