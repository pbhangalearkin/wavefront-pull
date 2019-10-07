import datetime
import pandas as pd
import calendar
import argparse

queries = {
        'inputsdm_count_per_container': 'avg(ts(dd.vRNI.UploadHandler.sdm, did="{}"), "_source")',
        'configstore_cache_miss_per_container': 'avg(ts(dd.vRNI.CachedConfigStore.getMiss, did="{}"), "_source")',
        'configstore_cache_hit_per_container': 'avg(ts(dd.vRNI.CachedConfigStore.getHit, did="{}"), "_source")',
        'doc_indexed_per_container': 'avg(ts(dd.vRNI.ConfigIndexerHelper.indexCount, did="{}"), "_source")',
        'program_time_per_container': 'avg(ts(dd.vRNI.GenericStreamTask.processorConsumption, did="{}"), "_source")'
    }

def argument_parser():
    global queries
    current_time = datetime.datetime.now()
    yesterdays_time = current_time - datetime.timedelta(days=1)

    parser = argparse.ArgumentParser()

    parser.add_argument("-did", type=str, help="did for wavefront", default="DP10XVX")  # "DP10XVX")
    parser.add_argument("-cs", "--current-start", type=str, help="Start of Current Time Frame(UTC)",
                        default=yesterdays_time.strftime("%Y-%m-%d-%H"))

    args = parser.parse_args()
    did = args.did
    info = "For DID = " + did + "\n"
    info += "Stats from: " + args.current_start + "\n"

    print(info)
    start_time = datetime.datetime.strptime(args.current_start, "%Y-%m-%d-%H")
    end_time = start_time + datetime.timedelta(days=1)
    run_time = to_epoch_range(start_time, end_time)

    for query in queries.keys():
        queries[query] = queries[query].format(did)
    return did, run_time


# converts to epoch. the start and end times are assumed to be in UTC timezone.
def to_epoch_range(start, end):
    start_epoch = calendar.timegm(start.timetuple())
    end_epoch = calendar.timegm(end.timetuple())
    return start_epoch * 1000, end_epoch * 1000


# Returns the epoch time of (start_day to end_day) wrt to TODAY
def get_timerange(start_day, end_day):
    today = datetime.date.today()
    start = today - datetime.timedelta(days=start_day)
    end = today - datetime.timedelta(days=end_day)

    return to_epoch_range(start, end)


# Returns the epoch time of (now() - 24hrs, now())
def timerange_last24hours():
    current_time = datetime.datetime.now()
    yesterday = current_time - datetime.timedelta(days=1)

    return to_epoch_range(yesterday, current_time)


def timerange_yesterday():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    return to_epoch_range(yesterday, today)


def timerange_daybeforeyesterday():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    daybefore = today - datetime.timedelta(days=2)

    return to_epoch_range(daybefore, yesterday)


def multiseries_to_stats(multiseries, df_tostats, queryname):
    """
    :param multiseries: A list of timeseries
    :param df_tostats: Function that takes a dataframe and returns a TaggedStats object
    :return: A list of TaggedStats for each series in the multiseries.
    """
    result = []
    for ts in multiseries:
        raw_data = ts.data
        tags = ts.tags

        assert (len(tags) == 1, 'Expected 1 tag, but found' + str(len(tags)))

        tag = list(tags.values())[0]
        result.append(df_tostats(to_df(raw_data, queryname), tag))

    return result


def to_df(raw_data, queryname):
    header = ['timestamp', queryname]
    return pd.DataFrame(data=raw_data, columns=header)


# Takes the wavefront api response and converts into a dataframe
# containing two colums [timestamp, value]
def response_tostats(api_response, df_tostats, queryname):
    """
    :param api_response: The response returned from wavefront
    :param df_tostats: A function that takes a dataframe and returns a stats
    represented by a pandas series.
    :return: A list of TaggedStats object for each tag present in the metric.
    """
    if api_response is None:
        print("ERROR: api_response is NULL, Maybe Timeout")
        return []
    timeseries = api_response.timeseries
    if timeseries is None:
        print("ERROR: timeseries not found for ", api_response.name)
        return []

    if len(timeseries) == 1:
        df = to_df(timeseries[0].data, queryname)
        return [df_tostats(df)]
    else:
        return multiseries_to_stats(timeseries, df_tostats, queryname)
