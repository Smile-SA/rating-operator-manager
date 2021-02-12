import unittest
import yaml

from rating.manager.rates import rate
from rating.manager import rules


class TestRating(unittest.TestCase):
    """Test and assert the rating mechanism is functional."""

    rules = yaml.safe_load("""
        rules:
            -
                name: rules_small
                labelSet:
                    instance_type: small
                    storage_type: ssd
                    gpu_accel: "false"
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
                    metric: request_memory
                    value: 0.0007
                    unit: GiB-hours
                -
                    metric: usage_memory
                    value: 0.0014
                    unit: GiB-hours
            -
                name: rules_medium
                labelSet:
                    instance_type: medium
                    storage_type: hdd
                    gpu_accel: "true"
                ruleset:
                -
                    metric: request_cpu
                    value: 0.0008
                    unit: core-hours
                -
                    metric: usage_cpu
                    value: 0.0025
                    unit: core-hours
                -
                    metric: request_memory
                    value: 0.0007
                    unit: GiB-hours
                -
                    metric: usage_memory
                    value: 0.002
                    unit: GiB-hours
            -
                name: rules_large
                labelSet:
                    instance_type: large
                    storage_type: hdd
                    gpu_accel: "false"
                ruleset:
                -
                    metric: request_cpu
                    value: 0.0009
                    unit: core-hours
                -
                    metric: usage_cpu
                    value: 0.0026
                    unit: core-hours
                -
                    metric: request_memory
                    value: 0.0008
                    unit: GiB-hours
                -
                    metric: usage_memory
                    value: 0.0022
                    unit: GiB-hours
            -
                name: rules_default
                ruleset:
                -
                    metric: request_cpu
                    value: 0.005
                    unit: core-hours
                -
                    metric: usage_cpu
                    value: 0.015
                    unit: core-hours
                -
                    metric: request_memory
                    value: 0.004
                    unit: GiB-hours
                -
                    metric: usage_memory
                    value: 0.012
                    unit: GiB-hours
    """)

    def test_rating_memory_frames(self):
        labels = {
            'instance_type': 'large',
            'storage_type': 'hdd',
            'gpu_accel': 'false'
        }
        _, rule = rules.find_match('usage_memory',
                                   labels,
                                   self.rules['rules'])
        rating = rate(rule, {'qty': 42})
        self.assertEqual(rating, 0.09240000000000001)

    def test_rating_cpu_frames(self):
        labels = {
            'instance_type': 'large',
            'storage_type': 'hdd',
            'gpu_accel': 'false'
        }
        _, rule = rules.find_match('request_cpu',
                                   labels,
                                   self.rules['rules'])
        rating = rate(rule, {'qty': 42})
        self.assertEqual(rating, 0.0378)

    def test_rating_no_match(self):
        _, rule = rules.find_match('pokemon',
                                   {},
                                   self.rules['rules'])
        rating = rate(rule, {'qty': 42})
        self.assertEqual(rating, None)
