import pickle

import pandas as pd

pickle_file_path = "ad_library_api/cache"

with open(pickle_file_path, "rb") as f:
    cache = pickle.load(f)
df = pd.concat(cache)
df.to_csv('./path_to_file.csv'.format("DE"), encoding='utf8')