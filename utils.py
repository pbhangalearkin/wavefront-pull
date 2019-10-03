import datetime
import pandas as pd
import calendar
import argparse

## Mapping between varca did and symphony environment
did_environment_mapping = {"DPSZ9NG" : "jazz"}

class RuntimeObjects():
    total_current_time = -1
    total_base_time = -1
    info = None


def argument_parser():
    current_time = datetime.datetime.now()
    yesterdays_time = current_time - datetime.timedelta(days=1)
    day_before_yesterdays_time = current_time - datetime.timedelta(days=2)

    parser = argparse.ArgumentParser()

    parser.add_argument("-did", type=str, help="did for wavefront", default="DPSZ9NG")  # "DP10XVX")
    parser.add_argument("-cs", "--current-start", type=str, help="Start of Current Time Frame(UTC)",
                        default=yesterdays_time.strftime("%Y-%m-%d-%H"))
    parser.add_argument("-ce", "--current-end", type=str, help="End of Current Time Frame(UTC)",
                        default=current_time.strftime("%Y-%m-%d-%H"))
    parser.add_argument("-bs", "--base-start", type=str, help="Start of Base Time Frame(UTC)",
                        default=day_before_yesterdays_time.strftime("%Y-%m-%d-%H"))
    parser.add_argument("-be", "--base-end", type=str, help="End of Base Time Frame(UTC)",
                        default=yesterdays_time.strftime("%Y-%m-%d-%H"))

    args = parser.parse_args()
    did = args.did
    if did in did_environment_mapping:
        environment = did_environment_mapping[did]
    else:
        environment = ""
    info = "For DID = " + did + " and Environment = " + environment + "\n"
    info += "Current stats from: " + args.current_start + " to " + args.current_end + "\n"
    info += "Base stats from: " + args.base_start + " to " + args.base_end + "\n"

    RuntimeObjects.info = info
    print(info)

    RuntimeObjects.total_current_time = (
            datetime.datetime.strptime(args.base_end, "%Y-%m-%d-%H") - datetime.datetime.strptime(args.base_start,
                                                                                                  "%Y-%m-%d-%H")).total_seconds();
    RuntimeObjects.total_base_time = (
            datetime.datetime.strptime(args.current_end, "%Y-%m-%d-%H") - datetime.datetime.strptime(args.current_start,
                                                                                                     "%Y-%m-%d-%H")).total_seconds();

    baseline_time = to_epoch_range(datetime.datetime.strptime(args.base_start, "%Y-%m-%d-%H"),
                                   datetime.datetime.strptime(args.base_end, "%Y-%m-%d-%H"))
    run_time = to_epoch_range(datetime.datetime.strptime(args.current_start, "%Y-%m-%d-%H"),
                              datetime.datetime.strptime(args.current_end, "%Y-%m-%d-%H"))
    return did, environment, baseline_time, run_time


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


def multiseries_to_stats(multiseries, df_tostats):
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
        result.append(df_tostats(to_df(raw_data), tag))

    return result


def to_df(raw_data):
    header = ['timestamp', 'value']
    return pd.DataFrame(data=raw_data, columns=header)


# Takes the wavefront api response and converts into a dataframe
# containing two colums [timestamp, value]
def response_tostats(api_response, df_tostats):
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
        print("ERROR: timeseries not found for " , api_response.name)
        return []

    if len(timeseries) == 1:
        df = to_df(timeseries[0].data)
        return [df_tostats(df)]
    else:
        return multiseries_to_stats(timeseries, df_tostats)
