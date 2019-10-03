from __future__ import print_function
import wavefront_api_client
from wavefront_api_client.rest import ApiException
from benchmark.utils import response_tostats, RuntimeObjects
from enum import Enum, auto
import pandas as pd
import numpy as np
from urllib3.exceptions import MaxRetryError

prod_config = wavefront_api_client.Configuration()
prod_config.host = "https://varca.wavefront.com"
prod_config.api_key['X-AUTH-TOKEN'] = '10a79735-3ed2-4cbd-bb3b-5ddecf2a06f7'
# create an instance of the API class
prod_api_instance = wavefront_api_client.QueryApi(wavefront_api_client.ApiClient(prod_config))

try:
    symphony_config = wavefront_api_client.Configuration()
    symphony_config.host = "https://symphony.wavefront.com"
    symphony_config.api_key['X-AUTH-TOKEN'] = 'e2b2f93e-0ce4-4757-8f4d-2a67e41ac57a'

    symphony_api_instance = wavefront_api_client.QueryApi(wavefront_api_client.ApiClient(symphony_config))
except Exception as e:
    symphony_api_instance = None

# Metrices for whom we take only top K candidates. 20 in this case
lst = ["Program time", "Denorm Latency By Object Type", "Input SDM", "Object Churn"]


# Priority of metrics. High priority metric breaches will
# be highlighted first
class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


# Various metric categories present in the system
class Category(Enum):
    UNKNOWN = 1
    SYSTEM = 2
    GRID = 3
    INDEXER = 4
    REST_API = 5
    UPTIME = 6
    SYMPHONY = 7


# Various skus in the system
class Sku(Enum):
    PLATFORM = 'platform'
    PROXY = 'proxy'


# Various processes in the system
class Process(Enum):
    RESTAPILAYER = ('restapilayer', Sku.PLATFORM)
    ELASTICSEARCH = ('elasticsearch', Sku.PLATFORM)
    SAMZA = ('samza-container', Sku.PLATFORM)
    SAASLISTENER = ('saaslistener', Sku.PLATFORM)
    LAUNCHER = ('launcher', Sku.PLATFORM)

    def process_name(self):
        return self.value[0]

    def sku(self):
        return self.value[1].value


# Class to define a metric object
class Metric:
    def __init__(self, name, query, category=Category.UNKNOWN, compare_with='mean', threshold=20,
                 lower_the_better=True, wavefront="varca"):
        """
        :param name: The name of the metric (e.g: Average message age)
        :param query: The wavefront query used to get the metric time series.
        :param compare_with: The metric to use for comparing with baseline, valid
        values are 'mean' or percentiles (e.g '90%').
        """
        self.name = name
        self.query = query
        self.priority = Priority.LOW
        self.category = category
        self.compare_with = compare_with
        self.threshold = threshold
        self.lower_the_better = lower_the_better
        self.wavefront = wavefront

    def set_priority(self, priority):
        self.priority = priority

    def set_category(self, category):
        self.category = category


class TaggedStats:
    """
    Stores the stats for a particular metric, tag combination.
    self.stats is a pandas series with the following keys:
    count, mean, std, min, 10%, 25%, 50%, 75%, 90% 95%, max
    """

    def __init__(self, tag, percentile_stats):
        self.tag = tag
        self.stats = percentile_stats

    def is_empty(self):
        return self.stats['count'] == 0


class TagMetricChangeResult:

    def __init__(self,
                 tag,
                 current_value,
                 baseline_value=None,
                 is_failure=False):
        """
        :param tag: the metric tag
        :param current_value: current value for this metric, tag combination
        :param baseline_value: baseline value for this metric, tag combination
        :param is_failure: boolean indicating if this is a failure
        """
        self.tag = tag
        self.baseline_value = baseline_value
        self.current_value = current_value
        self.is_failure = is_failure
        self.baseline_percentiles = None
        self.current_percentiles = None

    def __repr__(self):
        return "Tag : " + (self.tag if self.tag is not None else "None") + ", Base Value : " + str(
            self.baseline_value) + ", Current Value : " + str(self.current_value)

    def set_baseline_percentiles(self, percentiles):
        self.baseline_percentiles = percentiles

    def set_current_percentiles(self, percentiles):
        self.current_percentiles = percentiles


class TaggedValidationResult:
    """
    This is a the result of the metric evaluation for a metric. If a metric has multiple timeseries
    due to a tag, then this object stores the evaluation stats of all the tags for that metric.
    """

    def __init__(self, metric, tagged_stats):
        self.metric = metric
        self.run_stats = tagged_stats
        self.baseline_stats = None
        self.tag_to_change_results = None

    def set_baseline_stats(self, baseline_stats):
        self.baseline_stats = baseline_stats

    def get_filtered_tags(self):
        if self.metric.name in lst:
            # value_array = []
            # total_sum = 0
            # for tagged_stats in self.run_stats:
            #     total_sum += tagged_stats.stats["cumulative_value"]  # Getting Days reading
            #     value_array.append([tagged_stats.tag, tagged_stats.stats["cumulative_value"]])  # storing tags as well
            # value_array.sort(key=lambda x: x[1])  # sorting according to cumulative_value
            #
            # threshold = total_sum * 0.01  # to threshold
            # sum = 0
            # print(value_array)
            # for i in range(0, len(value_array)):
            #     sum += value_array[i][1]
            #     if sum > threshold:  # As soon as sum reaches thresholded value, break and store rest of tags
            #         break
            #     # print(sum,total_sum,i)
            # filtered_tags = [row[0] for row in value_array[i:]]
            value_array = []
            for tagged_stats in self.run_stats:
                value_array.append([tagged_stats.tag, tagged_stats.stats["cumulative_value"]])  # storing tags as well

            value_array.sort(key=lambda x: x[1])  # sorting according to cumulative_value
            # print(len(value_array), value_array)
            filtered_tags = [row[0] for row in value_array[-20:]]  # Top 20 candicates
            # print(filtered_tags)
            return filtered_tags
        else:
            return [row.tag for row in self.run_stats]

    def analyse(self):
        """
        Analyse the stats and identify pass/failure
        Returns nothing but stores the state of the analysis self.tag_to_change_results
        """
        filtered_tags = self.get_filtered_tags()
        tag_to_change_results = {}
        for tagged_stats in self.run_stats:
            tag = tagged_stats.tag
            if not tagged_stats.is_empty():
                if tagged_stats.tag in filtered_tags:
                    current_value = tagged_stats.stats[self.metric.compare_with]
                    change_result = TagMetricChangeResult(tag, current_value)
                    change_result.set_current_percentiles(tagged_stats.stats)
                    tag_to_change_results[tag] = change_result
        if self.baseline_stats is not None:
            for bl_tagged_stats in self.baseline_stats:
                tag = bl_tagged_stats.tag
                if not bl_tagged_stats.is_empty():
                    baseline_value = bl_tagged_stats.stats[self.metric.compare_with]
                    if tag in tag_to_change_results:
                        tag_to_change_results[tag].baseline_value = baseline_value
                elif tag in tag_to_change_results:
                    tag_to_change_results[tag].baseline_value = None
                elif tag in filtered_tags:
                    tag_to_change_results[tag] = TagMetricChangeResult(tag, None, None)
        self.mark_failures(tag_to_change_results)
        self.tag_to_change_results = tag_to_change_results

    def get_analysis_results(self):
        return self.tag_to_change_results

    def mark_failures(self, tag_to_change_results):
        for tag, change_result in tag_to_change_results.items():
            bv = change_result.baseline_value
            cv = change_result.current_value

            change_result.is_failure = False

            if bv is None and cv is None:
                change_result.is_failure = False
            elif bv is None or cv is None:
                change_result.is_failure = True
            elif self.metric.lower_the_better and cv < bv:
                change_result.is_failure = False
            elif (not self.metric.lower_the_better) and bv < cv:
                change_result.is_failure = False
            elif 100 * abs(bv - cv) / abs(bv) > self.metric.threshold:
                change_result.is_failure = True


class TaggedValidationResultUptime:
    """
    This is a the result of the metric evaluation for a metric. If a metric has multiple timeseries
    due to a tag, then this object stores the evaluation stats of all the tags for that metric.
    This is special class for all the uptime metrices. Is independent of baseline stats.
    """

    def __init__(self, metric, tagged_stats):
        self.metric = metric
        self.run_stats = tagged_stats
        self.tag_to_change_results = None

    def analyse(self):
        """
        Analyse the stats[restart_count] and identify pass/failure
        Returns nothing but stores the state of the analysis self.tag_to_change_results
        """
        tag_to_change_results = {}
        for tagged_stats in self.run_stats:
            tag = tagged_stats.tag
            if not tagged_stats.is_empty():
                current_value = tagged_stats.stats[self.metric.compare_with]
                change_result = TagMetricChangeResult(tag, current_value)
                change_result.set_current_percentiles(tagged_stats.stats)
                tag_to_change_results[tag] = change_result
        self.mark_failures(tag_to_change_results)
        self.tag_to_change_results = tag_to_change_results

    def get_analysis_results(self):
        return self.tag_to_change_results

    @staticmethod
    def mark_failures(tag_to_change_results):
        for tag, change_result in tag_to_change_results.items():
            cv = change_result.current_value

            change_result.is_failure = False

            if cv is None:
                change_result.is_failure = False
            elif cv > 0:
                change_result.is_failure = True


def validate_benchmark_run(
        metrics,
        run_timerange,
        baseline_timerange=None,
):
    """
    :param metrics: List of Metric objects
    :param run_timerange: (start, end) timestamps for the current run
    :param baseline_timerange: (start, end) timestamps for the baseline run.
    :return: list of ValidationResult objects
    """
    validation_results = []
    uptime_results = []
    for metric in metrics:
        print("Fetching metric : " + metric.name)
        current_run_response = query_wf(metric, run_timerange)
        current_run_stats = response_tostats(current_run_response, filtered_stats)

        if metric.category != Category.UPTIME:
            result = TaggedValidationResult(metric, current_run_stats)
            # print(current_run_stats[0].stats['restart'])
            if baseline_timerange:
                baseline_response = query_wf(metric, baseline_timerange)
                baseline_stats = response_tostats(baseline_response, filtered_stats)
                result.set_baseline_stats(baseline_stats)
            result.analyse()
            validation_results.append(result)
        else:
            result = TaggedValidationResultUptime(metric, current_run_stats)
            result.analyse()
            uptime_results.append(result)
        if metric.name == "Program time":
            result = grid_utilisation(current_run_stats, baseline_stats)
            validation_results.append(result)

    return validation_results, uptime_results


def grid_utilisation(current_run_stats, baseline_stats):
    current_utilisation = 0;
    base_utilisation = 0;
    for tagged_stats in current_run_stats:
        current_utilisation += tagged_stats.stats["cumulative_value"]
    for tagged_stats in baseline_stats:
        base_utilisation += tagged_stats.stats["cumulative_value"]

    grid_utilisation_metric = Metric("Grid Utilisation",
                                     'DUMMY',
                                     threshold=20, category=Category.GRID)
    # Unit for program time in miliseconds.
    current_stat = (current_utilisation * 100) / (1000 * RuntimeObjects.total_current_time)
    base_stat = (base_utilisation * 100) / (1000 * RuntimeObjects.total_base_time)
    result = TaggedValidationResult(grid_utilisation_metric, current_stat)
    result.set_baseline_stats(base_stat)

    mark_result = TagMetricChangeResult(tag=None, current_value=current_stat, baseline_value=base_stat,
                                        is_failure=False)
    result.tag_to_change_results = {None: mark_result}
    result.mark_failures(result.tag_to_change_results)
    return result


def query_wf(
        metric,
        time_range,
):
    """
    Query wavefront and return query results
    :param query_str: The wavefront query string
    :param time_range: Tuple of (start, end) timestamps
    :return: Query results
    """
    start_time = time_range[0]
    end_time = time_range[1]
    query_str = metric.query
    if metric.wavefront == "varca":
        wavefront_to_query = prod_api_instance
        granularity = 'h'
        summarization = 'MEAN'
    else:
        wavefront_to_query = symphony_api_instance
        granularity = 'd'
        summarization = 'MAX'
    # query_str = 'align(1'+granularity+', '+query_str+')'
    # print(query_str)
    # granularity = 'h'  # minutely granularity
    try:
        # Perform a charting query against Wavefront servers that
        # returns the appropriate points in the specified time window and granularity
        api_response = wavefront_to_query.query_api(
            q=query_str, s=start_time, g=granularity, e=end_time,
            i=True, auto_events=False, summarization=summarization,
            list_mode=True, strict=True, include_obsolete_metrics=False,
            sorted=False, cached=True)
        return api_response
    except ApiException as e:
        print("Exception when calling QueryApi->query_api: %s\n" % e)


# Returns stats as a pandas series
# The keys of the series are count,
# mean, std, min, 10%, 25%, 50%, 75%, 90% 95%, max
def stats(df, tag=None):
    """
    :param tag: The tag value of the timeseries or None if there is no tag
    :param df: dataframe containing the timeseries(time, values)
    :return: percentiles and min and max
    """

    # calculate linear difference in uptime. If difference fall to negative value,
    # It means program has restarted.
    x = df['value'].to_numpy()
    restart_count = (np.ediff1d(x, to_begin=0) < 0).sum()
    percentiles = df['value'].describe(
        percentiles=[0.10, 0.25, 0.5, 0.75, 0.9, 0.95])
    # Append the restart count value to the stats.
    # This is useful while doing uptime check for restarts.
    uptime = pd.Series([restart_count], index=['restart'])
    percentiles_uptime = percentiles.append(uptime, verify_integrity=True)
    return TaggedStats(tag, percentiles_uptime)


def filtered_stats(df, tag=None):
    x = df['value'].to_numpy()
    restart_count = (np.ediff1d(x, to_begin=0) < 0).sum()
    x = x[x >= 0]
    cumulative_value = x.sum()
    y = pd.DataFrame({'value': x})
    percentiles = y['value'].describe(
        percentiles=[0.10, 0.25, 0.5, 0.75, 0.9, 0.95])
    uptime = pd.Series([restart_count, cumulative_value], index=['restart', 'cumulative_value'])
    percentiles_uptime = pd.concat([percentiles, uptime])
    return TaggedStats(tag, percentiles_uptime)
