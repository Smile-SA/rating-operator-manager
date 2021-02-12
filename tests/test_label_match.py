import unittest

from rating.manager import rules

import yaml


class TestLabelMatch(unittest.TestCase):
    """Test the label matching aspect for the rating."""

    rules = yaml.safe_load("""
        -
            labelSet:
                instance_type: small
            ruleset:
            -
                metric: request_cpu
                value: 0.00075
                unit: core-hours
            -
                metric: usage_cpu
                value: 0.0015
                unit: core-hours
        -
            ruleset:
            -
                metric: request_cpu
                value: 0.0001
                unit: core-hours
        -
            ruleset:
            -
                metric: usage_cpu
                value: 0.0002
                unit: core-hours
    """)

    def test_matching_labels(self):
        labelset, rule = rules.find_match('usage_cpu',
                                          {
                                              'instance_type': 'small',
                                              'sldjalfksdjf': 'alsdkfjalsdkfj'
                                          },
                                          self.rules)

        self.assertEqual({'instance_type': 'small'}, labelset)
        self.assertEqual({
            'metric': 'usage_cpu',
            'value': 0.0015,
            'unit': 'core-hours',
        }, rule)

    def test_matching_default(self):
        labelset, rule = rules.find_match('usage_cpu',
                                          {
                                              'cpu_arch': 'z80',
                                          },
                                          self.rules)
        self.assertEqual({}, labelset)
        self.assertEqual({
            'metric': 'usage_cpu',
            'value': 0.0002,
            'unit': 'core-hours',
        }, rule)

    def test_matching_default_with_checked_label(self):
        labelset, rule = rules.find_match('usage_cpu',
                                          {
                                              'instance_type': 'foobar',
                                          },
                                          self.rules)

        self.assertEqual({}, labelset)

        self.assertEqual({
            'metric': 'usage_cpu',
            'value': 0.0002,
            'unit': 'core-hours',
        }, rule)

    def test_matching_wrong_metric(self):
        labelset, rule = rules.find_match('nothing',
                                          {
                                              'instance_type': 'pokemon',
                                          },
                                          self.rules)
        self.assertEqual({}, labelset)
        self.assertEqual({}, rule)
