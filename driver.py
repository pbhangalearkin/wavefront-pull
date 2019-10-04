import utils
import query as Query
import pickle
import sys
import numpy
numpy.set_printoptions(threshold=sys.maxsize)
did, time_range = utils.argument_parser()
#
# query_str = input("Metric to query : ")
# granuality = input("Enter Granuality[h/m/d] : ")
# output_file = input("Enter ouput file name : ")

queries = {
    'inputsdm_count_per_container' : 'avg(ts(dd.vRNI.UploadHandler.sdm, did="{}"), "_source")'.format(did),
    'configstore_cache_miss_per_container' : 'avg(ts(dd.vRNI.CachedConfigStore.getMiss, did="{}"), "_source")'.format(did),
    'configstore_cache_hit_per_container' : 'avg(ts(dd.vRNI.CachedConfigStore.getHit, did="{}"), "_source")'.format(did),
    'doc_indexed_per_container' : 'avg(ts(dd.vRNI.ConfigIndexerHelper.indexCount, did="{}"), "_source")'.format(did),
    'program_time_per_container' : 'avg(ts(dd.vRNI.GenericStreamTask.processorConsumption, did="{}"), "_source")'.format(did)
}

output = {}

granuality = 'm'
output_file = 'test'

for queryname in queries.keys():
    print("Fetching "+ queryname)
    api_response = Query.query_wf(queries[queryname], granuality,time_range)
    stats_output = utils.response_tostats(api_response,Query.filtered_stats)
    for stat in stats_output:
        print(stat.tag)
        if stat.tag in output:
            output[stat.tag][queryname] = stat.stats
        else:
            output[stat.tag] = {queryname : stat.stats}

with open('output.pickle', 'wb') as handle:
    pickle.dump(output, handle, protocol=pickle.HIGHEST_PROTOCOL)
# disk_util = Metric("disk utilization", mdiff(1m, avg(ts(dd.vRNI.UploadHandler.sdm, did="DPSZ9NG"), sdm))
#                    'avg(ts(dd.system.io.util, did="{}" and iid="*" and source="*" and role="platform" and device="dm-6"))'.format(
#                        did), threshold=20)
# message_age = Metric("message age",
#                      'avg(ts(dd.vRNI.GenericStreamTask.messageAge.mean, did="{}"))'.format(did), threshold=20)
# input_sdm = Metric("Input SDM", 'mdiff(1h, avg(ts(dd.vRNI.UploadHandler.sdm, did="{}"), sdm))'.format(did),
#                    threshold=20)
# # Grid metrics
# program_time = Metric("Program time",
#                       'mdiff(1h, avg(ts(dd.vRNI.GenericStreamTask.processorConsumption, did="{}"), pid))'.format(did),
#                       threshold=20, category=Category.GRID)
# object_churn = Metric("Object Churn",
#                       'mdiff(1h, sum(ts(dd.vRNI.ConfigStore.churn, did="{}"), ot, churn_type))'.format(did),
#                       threshold=20, category=Category.GRID)
# metric_cache_miss_rate = Metric("Miss rate",
#                                 'mdiff(1h, avg(ts(dd.vRNI.CachedAlignedMetricStore.miss_300.count, did="{}"))) * 100 / mdiff(1h, avg(ts(dd.vRNI.CachedAlignedMetricStore.gets_300.count, did="{}")))'.format(
#                                     did, did), threshold=20, category=Category.GRID)
# denorm_latency_by_ot = Metric("Denorm Latency By Object Type",
#                               'avg(ts(dd.vRNI.DenormComputationProgram.latency.mean, did="{}"), ot)'.format(did),
#                               threshold=20, category=Category.GRID)
# sdm_count_by_container = Metric("SDM count by container",
#                                'avg(ts(dd.vRNI.GenericStreamTask.sdm, did="{}"), "_source")'.format(did), threshold=20,
#                                category=Category.GRID)
# grid_metrics = [metric_cache_miss_rate, program_time, denorm_latency_by_ot, object_churn, sdm_count_by_container]
#
# # Indexer metrics
# index_lag_new = Metric("Indexer Lag",
#                        'ts(dd.vRNI.ConfigIndexerHelper.lag, did="{}")'.format(did), threshold=20,
#                        category=Category.INDEXER)
# index_lag_old = Metric("Indexer Lag(old)",
#                        'time()*1000 - ts(dd.vRNI.ConfigIndexerHelper.bookmark, did="{}")'.format(did), threshold=20,
#                        category=Category.INDEXER)
# indexed_docs_per_hour = Metric("Indexed docs per hour",
#                                'mdiff(1h, ts(dd.vRNI.ConfigIndexerHelper.indexCount, did="{}"))'.format(did),
#                                threshold=20, lower_the_better=False, category=Category.INDEXER)
# es_heap_usage_avg = Metric("ES Heap usage (Average)",
#                            'avg(ts(dd.jvm.mem.heap_used, did="{}"))'.format(did), threshold=20,
#                            category=Category.INDEXER)
# es_heap_usage_max = Metric("ES Heap usage (Max)",
#                            'max(ts(dd.jvm.mem.heap_used, did="{}"))'.format(did), compare_with='max', threshold=20,
#                            category=Category.INDEXER)
# gc_collection_time_to_es = Metric("GC Collection Time for ES/Hr",
#                                   'mdiff(1h, ts(dd.jvm.gc.collectors.*.collection_time, did="{}"))'.format(did),
#                                   threshold=20, category=Category.INDEXER)
# indexer_metrics = [
#     index_lag_old,
#     index_lag_new,
#     indexed_docs_per_hour,
#     es_heap_usage_avg,
#     es_heap_usage_max,
#     gc_collection_time_to_es]
# # uptime metrics
# uptime_metrics = []
# for p in Process:
#     m = Metric(p.sku() + '.' + p.process_name(),
#                'avg(ts(dd.system.processes.run_time.avg, did="{}" and sku={} and process_name={}), iid)'.format(did,
#                                                                                                                 p.sku(),
#                                                                                                                 p.process_name()),
#                compare_with='restart', threshold=20, category=Category.UPTIME)
#     uptime_metrics.append(m)
#
# # Symphony Metrics
# symphony_metrics = []
# # Only if corresponding environment in symphony exist for this did
# if environment is not "":
#     ui_response_time = Metric("UI Response Time",
#                               "ts(scaleperf.vrni.ui.responsetime, environment={})".format(environment),
#                               category=Category.SYMPHONY, wavefront="symphony")
#
#     symphony_metrics.append(ui_response_time)
#
# metrics = [disk_util, message_age, input_sdm]
# metrics.extend(symphony_metrics)
# metrics.extend(grid_metrics)
# metrics.extend(indexer_metrics)
# metrics.extend(uptime_metrics)
#
# validation_results, uptime_results = validate_benchmark_run(metrics, run_time, baseline_time)
# convert_to_csv(validation_results, uptime_results)
