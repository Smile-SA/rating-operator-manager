from typing import AnyStr, Dict, List
from rating.manager import utils


def rate(rule: Dict, frame: Dict) -> float or None:
    """
    Rate the frame with the given rule.

    :rule (Dict) A dictionary holding the rule to rate.
    :frame (Dict) The frame to rate, as a dict.

    Return the value of the rated frame, or none.
    """
    value = rule.get('value')
    if value is not None:
        return float(value) * frame['qty']
    return None


def convert_metrics_unit(metric_unit: AnyStr,
                         rating_unit: AnyStr,
                         qty: int) -> float:
    """
    Convert the metric unit according to configuration.

    :metric_unit (AnyStr) The metric to be converted.
    :rating_unit (AnyStr) Which conversion to apply.
    :qty (int) The value to be transformed.

    Return the converted value as a float.
    """
    try:
        return {
            ('byte-seconds', 'GiB-hours'): lambda x: (float(x) / 1024 ** 3) / 3600,
            ('core-seconds', 'core-hours'): lambda x: float(x) / 3600,
            ('byte', 'GiB'): lambda x: (float(x) / 1024 ** 3)
        }[(metric_unit, rating_unit)](qty)
    except KeyError:
        raise utils.ConfigurationException('Unsupported key in conversion')
