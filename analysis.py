import pickle
import sys
import numpy as np
import pandas as pd
np.set_printoptions(threshold=sys.maxsize)

with open('output.pickle', 'rb') as handle:
    b = pickle.load(handle)

job = 'job4-0'
x = pd.merge_asof(b[job]['program_time_per_container'], b[job]['configstore_cache_miss_per_container'], on='timestamp', allow_exact_matches=False)
x = pd.merge_asof(x, b[job]['configstore_cache_hit_per_container'], on='timestamp', allow_exact_matches=False)
x = pd.merge_asof(x, b[job]['doc_indexed_per_container'], on='timestamp', allow_exact_matches=False)
x = pd.merge_asof(x, b[job]['inputsdm_count_per_container'], on='timestamp', allow_exact_matches=False)

print(x)
#print(b[job]['configstore_cache_miss_per_container'].shape)

# x = np.concatenate(x,b[job]['configstore_cache_hit_per_container'],b[job]['doc_indexed_per_container'])
