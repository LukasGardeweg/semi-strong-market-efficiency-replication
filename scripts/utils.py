import pandas as pd
def split_datasets(datasets):
    """Split datasets into required periods."""
    all_split_datasets = {}
    for name, df in datasets.items():
        if name == "01_across_time":
            all_split_datasets[name] = {
                "seasons_06_17": df[df["Season"].isin(
                    ["06_07","07_08","08_09","09_10","10_11","11_12","12_13","13_14","14_15","15_16","16_17"]
                )],
                "seasons_17_20": df[df["Season"].isin(["17_18","18_19","19_20"])]
            }
        else:
            all_split_datasets[name] = {
                "seasons_01_04": df[df["Season"].isin(["01_02","02_03","03_04","04_05"])],
                "season_05_06": df[df["Season"] == "05_06"]
            }
    return all_split_datasets

def clean_and_long_format(df):
    """Clean dataframe and transform to long format (home + away rows)."""
    if df.empty:
        return df
    df = df.dropna()
    if "avg_home_spectator" in df.columns and "avg_away_spectator" in df.columns:
        df = df[(df["avg_home_spectator"] != 0) & (df["avg_away_spectator"] != 0)]
    df = df.copy()
    df["MATCH_ID"] = range(1, len(df) + 1)

    home_df = df.copy()
    home_df["Team"] = home_df["HomeTeam"]
    home_df["HOME"] = 1
    home_df["spectators1"] = home_df["avg_home_spectator"]
    home_df["spectators2"] = home_df["avg_away_spectator"]

    away_df = df.copy()
    away_df["Team"] = away_df["AwayTeam"]
    away_df["HOME"] = 0
    away_df["spectators1"] = away_df["avg_away_spectator"]
    away_df["spectators2"] = away_df["avg_home_spectator"]

    cols = ["MATCH_ID","Div","Season","Date","Team","HOME",
            "FTHG","FTAG","FTR","HTHG","HTAG","HTR",
            "HomeBookmaker","DrawBookmaker","AwayBookmaker","spectators1","spectators2"]
    return pd.concat([home_df[cols], away_df[cols]], ignore_index=True)