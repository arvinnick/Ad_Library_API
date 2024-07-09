import os

import pandas as pd

dfs = []
for file in os.listdir("./output/"):
    if file.endswith(".csv"):
        dfs.append(pd.read_csv(os.path.join("./output", file), index_col=0))

df = pd.concat(dfs)
df.drop_duplicates(subset = ["id"], inplace=True)
df.to_csv("./output/output.csv")
