from __future__ import print_function
import wavefront_api_client
from wavefront_api_client.rest import ApiException
from enum import Enum, auto
import pandas as pd
import numpy as np

prod_config = wavefront_api_client.Configuration()
prod_config.host = "https://varca.wavefront.com"
prod_config.api_key['X-AUTH-TOKEN'] = '10a79735-3ed2-4cbd-bb3b-5ddecf2a06f7'
# create an instance of the API class
prod_api_instance = wavefront_api_client.QueryApi(wavefront_api_client.ApiClient(prod_config))


# Metrices for whom we take only top K candidates. 20 in this case

class TaggedStats:
    """
    Stores the stats for a particular metric, tag combination.
    self.stats is a pandas series with the following keys:
    count, mean, std, min, 10%, 25%, 50%, 75%, 90% 95%, max
    """

    def __init__(self, tag, stats):
        self.tag = tag
        self.stats = stats

    # def __repr__(self):
    #     return ""


def query_wf(
        query_str,
        granularity,
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
    wavefront_to_query = prod_api_instance
    summarization = 'MEAN'
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

def filtered_stats(df, tag=None):
    x = df['value'].to_numpy()
    return TaggedStats(tag, x)
