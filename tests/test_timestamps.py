from typing import Dict, List, Union
import unittest

from rating.manager.bisect import get_closest_configs_bisect

import yaml


def generate_test_from_timestamp(timestamp: int) -> Union[int, Dict, Dict]:
    """Generate multiple configurations procedurally, to be tested afterward."""
    rules = """
            rules:
            -
                name: group_m1
                labelSet:
                    test: true
                ruleset:
                -
                    metric: request_cpu
                    value: {}
                    unit: core-hours
                -
                    metric: usage_cpu
                    value: {}
                    unit: core-hours
            -
                name: group_m2
                labelSet:
                    test: false
                ruleset:
                -
                    metric: request_cpu
                    value: {}
                    unit: core-hours
                -
                    metric: usage_cpu
                    value: {}
                    unit: core-hours
    """.format(timestamp / 100,
               timestamp / 10,
               timestamp / 50,
               timestamp / 5)

    metrics = """
        metrics:
            rating_request_cpu:
                report_name: pod-cpu-request-hourly
                presto_table: report_metering_pod_cpu_request_hourly
                presto_column: pod_request_cpu_core_seconds
                unit: core-seconds

            rating_usage_cpu:
                report_name: pod-cpu-usage-hourly
                presto_table: report_metering_pod_cpu_usage_hourly
                presto_column: pod_usage_cpu_core_seconds
                unit: core-seconds

            rating_request_memory:
                report_name: pod-memory-request-hourly
                presto_table: report_metering_pod_memory_request_hourly
                presto_column: pod_request_memory_byte_seconds
                unit: byte-seconds

            rating_usage_memory:
                report_name: pod-memory-usage-hourly
                presto_table: report_metering_pod_memory_usage_hourly
                presto_column: pod_usage_memory_byte_seconds
                unit: byte-seconds
    """
    return (int(timestamp), (yaml.safe_load(rules),
                             yaml.safe_load(metrics)))


def generate_tests_fixture(size: int) -> List[Union[Dict, Dict]]:
    """Generate the configurations for the tests."""
    configurations = []
    base = generate_test_from_timestamp(1)
    configurations.append(base)
    for idx in range(1, size):
        config = generate_test_from_timestamp(idx * size / 10)
        configurations.append(config)
    return configurations


class TestConfigs(unittest.TestCase):
    """Test the configuration matching bisection algorithm."""

    size = 10000
    configurations = generate_tests_fixture(size)
    timestamps = tuple(ts[0] for ts in configurations)

    def test_find_closest_timestamp_bisect_zero(self):
        timestamp = 0
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 0)

    def test_find_closest_timestamp_bisect_one(self):
        timestamp = 1
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 0)

    def test_find_closest_timestamp_bisect_begin(self):
        timestamp = 250
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 1)

    def test_find_closest_timestamp_bisect_middle(self):
        timestamp = 694200
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 695)

    def test_find_closest_timestamp_bisect_end(self):
        timestamp = 999629
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 1000)

    def test_find_closest_timestamp_bisect_last(self):
        timestamp = 100000
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 100)

    def test_find_closest_timestamp_bisect_over(self):
        timestamp = 19231123123
        result = get_closest_configs_bisect(timestamp,
                                                   self.timestamps)
        self.assertEqual(result, 9999)

    def test_find_in_small(self):
        timestamps = (0, 1576663550, 1576675754, 1576678772)
        timestamp = 1576672457
        result = get_closest_configs_bisect(timestamp,
                                                   timestamps)
        self.assertEqual(result, 2)
