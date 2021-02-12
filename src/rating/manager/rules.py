from typing import AnyStr, Dict, List, Union
from rating.manager import utils


def check_label_match(frame_labels: Dict, labelset: Dict) -> bool:
    """
    Check that the labels of the frame matches the labels of the rules.

    :frame_labels (Dict) The labels of the frame.
    :labelset (Dict) The labels of the ruleset.

    Return a boolean describing the status of the match.
    """
    for key, value in labelset.items():
        if frame_labels.get(key) != value:
            return False
    return True


def find_match(metric: AnyStr,
               frame_labels: Dict,
               rules: Dict) -> Union[Dict, Dict]:
    """
    Find a match between the frame labels and all the rules labels.

    Helps to find the adequate ruleset to use to rate the frame.

    :metric (AnyStr) The metric name to be matched in rules.
    :frame_labels (Dict) The labels of the frame.
    :rules (Dict) The ruleset to iterate on.

    Return dictionaries containing the rules to use, and the matched labelset.
    """
    for ruleset in rules:
        labelset = ruleset.get('labelSet', {})
        rulelist = ruleset.get('ruleset', [])
        for rule in rulelist:
            if rule['metric'] != metric:
                continue
            if check_label_match(frame_labels, labelset):
                return labelset, rule
    return {}, {}


def validate_value(value: AnyStr) -> bool:
    """Validate the value."""
    return isinstance(value, str) and utils.is_valid_against(value, '^[a-zA-Z0-9-_]+$')


def ensure_rules_config(ruleset: List[Dict]):
    """
    Iterate over the ruleset to validate its content.

    :ruleset (List[Dict]) A list of dictionary to be validated.
    """
    accepted_rules_keys = {'metric', 'value', 'unit'}
    for entry in ruleset:
        pair_checking = []

        # Rules checking
        rules = entry.get('ruleset')
        if not rules or len(rules) == 0:
            raise utils.ConfigurationException(
                'No rules provided')
        for rule in rules:
            if rule in pair_checking:
                raise utils.ConfigurationException(
                    'Duplicated (metric, value, unit)',
                    rule)
            # Keys checking
            keys = set(rule.keys())
            if keys != accepted_rules_keys:
                raise utils.ConfigurationException(
                    'Wrong key in ruleset',
                    keys)
            # Values checking
            for value in rule.values():
                if isinstance(value, (int, float)):
                    continue
                elif not validate_value(value):
                    raise utils.ConfigurationException(
                        'Invalid value in ruleset',
                        value
                    )
            pair_checking.append(rule)

        # Labels checking
        labels = entry.get('labels')
        if not labels:
            continue
        for value in labels.values():
            if not isinstance(value, (str, int, float)):
                raise utils.ConfigurationException('Wrong type for label', value)
