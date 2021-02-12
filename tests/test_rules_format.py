import unittest

from rating.manager import rules
from rating.manager import utils

import yaml


class TestRulesFormat(unittest.TestCase):
    """Test and assert that the format validation functions are correct."""

    def test_ruleset_base_config(self):
        rules_test = yaml.safe_load("""
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
        rules.ensure_rules_config(rules_test)

    def test_ruleset_with_labelset(self):
        rules_test = yaml.safe_load("""
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
        """)

        rules.ensure_rules_config(rules_test)

    def test_ruleset_wrong_key_rules(self):
        rules_test = yaml.safe_load("""
        -
            name: rules_large
            labelSet:
                instance_type: large
                storage_type: hdd
                gpu_accel: "false"
            ruleset:
            -
                pokemon: ivysaur
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
        """)

        with self.assertRaisesRegex(utils.ConfigurationException,
                                    ('Wrong key in rules')):
            rules.ensure_rules_config(rules_test)

    def test_ruleset_without_labelset(self):
        rules_test = yaml.safe_load("""
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

        rules.ensure_rules_config(rules_test)

    def test_ruleset_duplicated_rule(self):
        rules_test = yaml.safe_load("""
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
            -
                metric: usage_memory
                value: 0.012
                unit: GiB-hours
        """)

        with self.assertRaisesRegex(utils.ConfigurationException,
                                    'Duplicated'):
            rules.ensure_rules_config(rules_test)

    def test_ruleset_no_rules(self):
        rules_test = yaml.safe_load("""
        -
            name: rules_default
            labelSet:
                thisis: atest
        """)

        with self.assertRaisesRegex(utils.ConfigurationException,
                                    'No rules'):
            rules.ensure_rules_config(rules_test)

    def test_ruleset_wrong_value(self):
        rules_test = yaml.safe_load("""
        -
            ruleset:
            -
                metric: request_memory;
                value: 0.012
                unit: GiB-hours
        """)

        with self.assertRaisesRegex(utils.ConfigurationException,
                                    'Invalid value'):
            rules.ensure_rules_config(rules_test)
