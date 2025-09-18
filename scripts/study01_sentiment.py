from utils import split_datasets, clean_and_long_format, process_bookprob_and_attendance
import pandas as pd
import numpy as np

# -------------------------------
# CHUNK 1: Load datasets
# -------------------------------

base_url = "https://raw.githubusercontent.com/LukasGardeweg/semi-strong-market-efficiency-replication/main/data/"
files = [
    "01_across_datasource.csv",
    "01_across_samples.csv",
    "01_across_time.csv",
    "01_reproduction.csv"
]

datasets = {
    file.split(".")[0]: pd.read_csv(base_url + file, sep=";", encoding='utf-8-sig').rename(columns=lambda x: x.strip())
    for file in files
}

all_split_datasets = split_datasets(datasets)
all_stats = {}

# Apply cleaning + long format
all_split_datasets_long = {
    name: {split_name: clean_and_long_format(df) for split_name, df in splits.items()}
    for name, splits in all_split_datasets.items()
}

all_split_datasets_long = {}
for name, splits in all_split_datasets.items():
    long_splits = {}
    for split_name, df in splits.items():
        long_splits[split_name] = clean_and_long_format(df)
        print(f"{name} - {split_name}: {len(long_splits[split_name])} rows (long format)")
    all_split_datasets_long[name] = long_splits

all_split_datasets_processed = {}
all_stats = {}

for name, splits in all_split_datasets.items():
    processed_splits = {}
    stats_splits = {}

    for split_name, df in splits.items():
        # Schritt 1: Clean + Long-Format
        df_long = clean_and_long_format(df)

        # Schritt 2: Berechne Bookprobs, Attendance & Stats
        df_processed, stats = process_bookprob_and_attendance(df_long)

        # Ergebnisse speichern
        processed_splits[split_name] = df_processed
        stats_splits[split_name] = stats

        # Direkt ausgeben
        print(f"\n{name} - {split_name}")
        for k, v in stats.items():
            print(f"  {k}: {v:.4f}" if isinstance(v, (int, float)) else f"  {k}: {v}")

    all_split_datasets_processed[name] = processed_splits
    all_stats[name] = stats_splits
