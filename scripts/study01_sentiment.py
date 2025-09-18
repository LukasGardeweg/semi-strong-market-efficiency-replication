import pandas as pd

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


all_split_datasets = {}

for name, df in datasets.items():
    if name == "01_across_time":  # Sonderregel für dieses Dataset
        df_part1 = df[df["Season"].isin(["06_07", "07_08", "08_09", "09_10", "10_11", "11_12", "12_13", "13_14", "14_15", "15_16", "16_17"])]
        df_part2 = df[df["Season"].isin(["17_18", "18_19", "19_20"])]
        all_split_datasets[name] = {
            "special_period1": df_part1,
            "special_period2": df_part2
        }
    else:  # Standard-Aufteilung für alle anderen Datasets
        df_part1 = df[df["Season"].isin(["01_02", "02_03", "03_04", "04_05"])]
        df_part2 = df[df["Season"] == "05_06"]
        all_split_datasets[name] = {
            "0102_0405": df_part1,
            "0506": df_part2
        }
# Alle Zeilen mit fehlenden Werten löschen
df = df.dropna()

# Zeilen löschen, bei denen avg_home_spectator oder avg_away_spectator 0 ist
df = df[(df["avg_home_spectator"] != 0) & (df["avg_away_spectator"] != 0)]

# Wenn du die Änderungen direkt im split-Dictionary übernehmen willst:
all_split_datasets_cleaned = {}
for name, splits in all_split_datasets.items():
    cleaned_splits = {}
    for split_name, df in splits.items():
        df_clean = df.dropna()
        if "avg_home_spectator" in df.columns and "avg_away_spectator" in df.columns:
            df_clean = df_clean[(df_clean["avg_home_spectator"] != 0) & (df_clean["avg_away_spectator"] != 0)]
        cleaned_splits[split_name] = df_clean
    all_split_datasets_cleaned[name] = cleaned_splits

def clean_and_long_format(df):
    """Clean dataframe and transform to long format (home + away rows)."""
    if df.empty:
        return df  # falls ein Split leer ist

    # Cleaning
    df = df.dropna()
    if "avg_home_spectator" in df.columns and "avg_away_spectator" in df.columns:
        df = df[(df["avg_home_spectator"] != 0) & (df["avg_away_spectator"] != 0)]

    # MATCH_ID
    df = df.copy()
    df["MATCH_ID"] = range(1, len(df) + 1)

    # Home rows
    home_df = df.copy()
    home_df["Team"] = home_df["HomeTeam"]
    home_df["HOME"] = 1
    home_df["spectators1"] = home_df["avg_home_spectator"]
    home_df["spectators2"] = home_df["avg_away_spectator"]

    # Away rows
    away_df = df.copy()
    away_df["Team"] = away_df["AwayTeam"]
    away_df["HOME"] = 0
    away_df["spectators1"] = away_df["avg_away_spectator"]
    away_df["spectators2"] = away_df["avg_home_spectator"]

    # Select columns
    cols = [
        "MATCH_ID", "Div", "Season", "Date", "Team", "HOME",
        "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR",
        "HomeBookmaker", "DrawBookmaker", "AwayBookmaker", "spectators1", "spectators2"
    ]
    home_df = home_df[cols]
    away_df = away_df[cols]

    # Concatenate
    return pd.concat([home_df, away_df], ignore_index=True)

all_split_datasets_long = {}
for name, splits in all_split_datasets.items():
    long_splits = {}
    for split_name, df in splits.items():
        long_splits[split_name] = clean_and_long_format(df)
        print(f"{name} - {split_name}: {len(long_splits[split_name])} rows (long format)")
    all_split_datasets_long[name] = long_splits