import unittest

from rating.manager.metrics import ensure_metrics_config
from rating.manager.utils import ConfigurationException


class TestMetrics(unittest.TestCase):
    """Test the metrics part of the rating configuration."""

    def test_flattened_metrics_declaration(self):
        metrics_dict = {
            'request_cpu': {
                'report_name': 'pod-cpu-request-hourly',
                'presto_table': 'report_metering_pod_cpu_request_hourly',
                'presto_column': 'pod_request_cpu_core_seconds',
                'unit': 'core-seconds'
            },
            'usage_cpu': {
                'report_name': 'pod-cpu-usage-hourly',
                'presto_table': 'report_metering_pod_cpu_usage_hourly',
                'presto_column': 'pod_usage_cpu_core_seconds',
                'unit': 'core-seconds'
            },
            'request_memory': {
                'report_name': 'pod-memory-request-hourly',
                'presto_table': 'report_metering_pod_memory_request_hourly',
                'presto_column': 'pod_request_memory_core_seconds',
                'unit': 'byte-seconds'
            },
            'usage_memory': {
                'report_name': 'pod-memory-usage-hourly',
                'presto_table': 'report_metering_pod_memory_usage_hourly',
                'presto_column': 'pod_usage_memory_core_seconds',
                'unit': 'byte-seconds'
            }
        }
        config = ensure_metrics_config(metrics_dict)
        self.assertEqual(config, metrics_dict)

    def test_unsupported_key_metrics_declaration(self):
        metrics_dict = {
            'request_cpu': {
                'test': 'pod-cpu-request-hourly',
                'presto_table': 'report_metering_pod_cpu_request_hourly',
                'presto_column': 'pod_request_cpu_core_seconds',
                'unit': 'core-seconds'
            },
            'usage_cpu': {
                'report_name': 'pod-cpu-usage-hourly',
                'presto_table': 'report_metering_pod_cpu_usage_hourly',
                'presto_column': 'pod_usage_cpu_core_seconds',
                'unit': 'core-seconds'
            },
            'request_memory': {
                'report_name': 'pod-memory-request-hourly',
                'presto_table': 'report_metering_pod_memory_request_hourly',
                'presto_column': 'pod_request_memory_core_seconds',
                'unit': 'byte-seconds'
            },
            'usage_memory': {
                'report_name': 'pod-memory-usage-hourly',
                'presto_table': 'report_metering_pod_memory_usage_hourly',
                'presto_column': 'pod_usage_memory_core_seconds',
                'unit': 'byte-seconds'
            }
        }
        with self.assertRaisesRegex(ConfigurationException,
                                    'Unsupported key in metrics definition'):
            ensure_metrics_config(metrics_dict)

    def test_unsupported_unit_metrics_declaration(self):
        metrics_dict = {
            'request_cpu': {
                'report_name': 'pod-cpu-request-hourly',
                'presto_table': 'report_metering_pod_cpu_request_hourly',
                'presto_column': 'pod_request_cpu_core_seconds',
                'unit': 'some-unit'
            },
            'usage_cpu': {
                'report_name': 'pod-cpu-usage-hourly',
                'presto_table': 'report_metering_pod_cpu_usage_hourly',
                'presto_column': 'pod_usage_cpu_core_seconds',
                'unit': 'core-seconds'
            },
            'request_memory': {
                'report_name': 'pod-memory-request-hourly',
                'presto_table': 'report_metering_pod_memory_request_hourly',
                'presto_column': 'pod_request_memory_core_seconds',
                'unit': 'byte-seconds'
            },
            'usage_memory': {
                'report_name': 'pod-memory-usage-hourly',
                'presto_table': 'report_metering_pod_memory_usage_hourly',
                'presto_column': 'pod_usage_memory_core_seconds',
                'unit': 'byte-seconds'
            }
        }
        with self.assertRaisesRegex(ConfigurationException,
                                    'Unsupported unit'):
            ensure_metrics_config(metrics_dict)
