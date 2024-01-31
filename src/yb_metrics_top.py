#!/usr/bin/env python3

import requests
import socket
import os
import re
import time

from collections import defaultdict


HORIZ_LINE = '-' * 80

TABLE_NAME_TAG = 'table_name'

METRIC_PREFIXES = (
  'log_append_latency',
  'log_group_commit_latency',
  'log_sync_latency'
)

SUM_OR_COUNT_RE = re.compile('^([a-z_]+)_(count|sum)$')

# log_sync_latency_sum{table_id="000033bd000030008000000000004029",table_name="warehouse",namespace_name="yugabyte",table_type="PGSQL_TABLE_TYPE",metric_type="table",exported_instance="yb-dev-mbautin8-543633601022541990-n1"} 434852 1706682229466
METRIC_LINE_RE = re.compile(''.join([
    r'^',
    r'([a-zA-Z_]+)',
    r'[{]',
    r'([^}]+)',
    r'[}]',
    r' ',
    r'(-?[0-9]+)',
    r' ',
    r'(-?[0-9]+)',
    r'$'
]))

TAG_RE = re.compile('^([a-zA-Z_]+)="([^"]+)"$')


def has_correct_prefix(line):
    for prefix in METRIC_PREFIXES:
        if line.startswith(prefix + '_'):
            return True
    return False


def get_metrics():
    host = socket.gethostname()
    ip = socket.gethostbyname(host)
    port = 9000
    metrics_url = f'http://{ip}:{port}/prometheus-metrics'
    tls_dir=os.path.expanduser('~/yugabyte-tls-config')
    cert_path = os.path.join(tls_dir, f'node.{ip}.crt')
    key_path = os.path.join(tls_dir, f'node.{ip}.key')
    response = requests.get(metrics_url, verify=False)
    metrics = []
    common_timestamp = None
    for line in response.text.split('\n'):
        if line.startswith('#'):
            continue
        if not has_correct_prefix(line):
            continue
        m = METRIC_LINE_RE.match(line)
        assert(m)
        metric_name, tags_str, value, timestamp = m.groups()
        value = int(value)
        if common_timestamp is None:
            timestamp = common_timestamp
        else:
            assert(timestamp == common_timestamp)
        tag_dict = {}
        for tag_str in tags_str.split(","):
            tag_m = TAG_RE.match(tag_str)
            tag_key, tag_value = tag_m.groups()
            if tag_key == 'exported_instance':
                continue
            tag_dict[tag_key] = tag_value
        metrics.append((metric_name, tag_dict, value))
    return metrics


def aggregate_by_tag(metrics, tag_name):
    key_and_tag_to_total = {}
    for metric in metrics:
        key, tag_dict, value = metric
        tag_value = tag_dict[tag_name]
        key_and_tag = (key, tag_value)
        key_and_tag_to_total[key_and_tag] = key_and_tag_to_total.get(key_and_tag, 0) + value
    return key_and_tag_to_total


def compute_delta(m1, m2):
    m1_agg = aggregate_by_tag(m1, TABLE_NAME_TAG)
    m2_agg = aggregate_by_tag(m2, TABLE_NAME_TAG)
    result_by_key_prefix_and_tag = defaultdict(dict)
    for key_and_tag in m1_agg.keys() & m2_agg.keys():
        key, tag = key_and_tag
        match = SUM_OR_COUNT_RE.match(key)
        assert(match)
        key_prefix, count_or_sum = match.groups()
        v1 = m1_agg[key_and_tag]
        v2 = m2_agg[key_and_tag]
        result_by_key_prefix_and_tag[(key_prefix, tag)][count_or_sum] = v2 - v1
    result = {}
    for key_prefix_and_tag, details in result_by_key_prefix_and_tag.items():
        s = details['sum']
        c = details['count']
        key_prefix, tag = key_prefix_and_tag
        if key_prefix not in result:
            result[key_prefix] = {}
        result[key_prefix][tag] = (s, c, (s / c if c != 0 else 0))
    return result


def main():
    interval_sec = 60
    print('TIME INTERVAL: %d' % interval_sec)
    m1 = get_metrics()
    time.sleep(interval_sec)
    m2 = get_metrics()
    rates = compute_delta(m1, m2)
    for metric_name, metric_data in sorted(rates.items()):
        print(HORIZ_LINE)
        print(metric_name)
        print(HORIZ_LINE)
        metric_data_items = sorted(
                metric_data.items(),
                key=lambda item: item[1][2],
                reverse=True)
        table_format_str = '%-20s %-10d %-10d %-10.6f'
        header_format_str = '%-20s %-10s %-10s %-17s'
        print(header_format_str % ('TABLE', 'SUM', 'COUNT', 'RATE'))
        print(HORIZ_LINE)
        for item in metric_data_items:
            table_name, details = item
            sum_value, count_value, rate = details
            print(table_format_str % (table_name, sum_value, count_value, rate))
        print()



if __name__ == '__main__':
    main()
