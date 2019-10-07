import pickle
import sys
import numpy as np
import utils
import pandas as pd
from sklearn.linear_model import LinearRegression

np.set_printoptions(threshold=sys.maxsize)

with open('output.pickle', 'rb') as handle:
    b = pickle.load(handle)

data = np.zeros(shape=(1,6))
for job in b['program_time_per_container']:
    merged_panda_series = b['program_time_per_container'][job]
    for query in utils.queries.keys():
        if query is not 'program_time_per_container':
            if job in b[query]:
                merged_panda_series = pd.merge_asof(merged_panda_series,b[query][job], on='timestamp', allow_exact_matches=False)
            else:
                merged_panda_series[query] = merged_panda_series['timestamp']*0
    merged_panda_series = merged_panda_series[sorted(merged_panda_series.columns)]
    merged_panda_series_numpy = merged_panda_series.to_numpy()
    merged_panda_series_numpy[np.isnan(merged_panda_series_numpy)] = 0
    data = np.concatenate((data,merged_panda_series_numpy))
data = data[1:]

program_time = data[:,4]
features = data[:,np.arange(4)]
reg = LinearRegression().fit(features, program_time)
print(reg.score(features,program_time))
# reg = LinearRegression().fit(data[], y)
    #print(merged_panda_series.to_numpy())
            # else:
            #     print(merged_panda_series)
            #     merged_panda_series = pd.merge_asof(merged_panda_series, , on='timestamp', allow_exact_matches=False)
            #     print (merged_panda_series)
            #     break
# x = pd.merge_asof(b[job]['program_time_per_container'], b[job]['configstore_cache_miss_per_container'], on='timestamp', allow_exact_matches=False)
# x = pd.merge_asof(x, b[job]['configstore_cache_hit_per_container'], on='timestamp', allow_exact_matches=False)
# x = pd.merge_asof(x, b[job]['doc_indexed_per_container'], on='timestamp', allow_exact_matches=False)
# x = pd.merge_asof(x, b[job]['inputsdm_count_per_container'], on='timestamp', allow_exact_matches=False)
#
# print(x)
#print(b[job]['configstore_cache_miss_per_container'].shape)

# x = np.concatenate(x,b[job]['configstore_cache_hit_per_container'],b[job]['doc_indexed_per_container'])
