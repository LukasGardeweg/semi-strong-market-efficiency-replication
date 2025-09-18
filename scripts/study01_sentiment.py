import pandas as pd

base_url = "https://github.com/LukasGardeweg/semi-strong-market-efficiency-replication.git"
files = [
    "01_across_datasource.csv",
    "02_across_samples.csv",
    "03_across_time.csv",
    "04_reproduction.csv"
]

datasets = {file.split(".")[0]: pd.read_csv(base_url + file) for file in files}