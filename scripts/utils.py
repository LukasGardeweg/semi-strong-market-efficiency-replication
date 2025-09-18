import pandas as pd
import numpy as np
def split_datasets(datasets):
    """Split datasets into required periods."""
    all_split_datasets = {}
    for name, df in datasets.items():
        if name == "01_across_time":
            all_split_datasets[name] = {
                "seasons_0607_1617": df[df["Season"].isin(
                    ["06_07","07_08","08_09","09_10","10_11","11_12","12_13","13_14","14_15","15_16","16_17"]
                )],
                "seasons_17_20": df[df["Season"].isin(["17_18","18_19","19_20"])]
            }
        else:
            all_split_datasets[name] = {
                "seasons_0102_0405": df[df["Season"].isin(["01_02","02_03","03_04","04_05"])],
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


def process_bookprob_and_attendance(df: pd.DataFrame):
    """
    Berechnet Buchmacherwahrscheinlichkeiten, Overround, DIFFATTEND und Outcome-Variablen
    analog zum R-Code.

    Args:
        df (pd.DataFrame): Long-format DataFrame mit Odds und Zuschauerzahlen.

    Returns:
        df (pd.DataFrame): DataFrame mit zusätzlichen Spalten.
        stats (dict): Dictionary mit beschreibenden Statistiken.
    """

    # Sicherstellen, dass Division durch 0 abgefangen wird
    df = df.copy()

     # Umwandlung in numerische Werte
    odds_cols = ["HomeBookmaker", "DrawBookmaker", "AwayBookmaker"]
    for col in odds_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        
    df["overround"] = (1/df["HomeBookmaker"] + 1/df["DrawBookmaker"] + 1/df["AwayBookmaker"]) - 1

    df["bookprob_home"] = (1/df["HomeBookmaker"]) / (df["overround"] + 1)
    df["bookprob_away"] = (1/df["AwayBookmaker"]) / (df["overround"] + 1)
    df["bookprob_draw"] = (1/df["DrawBookmaker"]) / (df["overround"] + 1)

    # BOOKPROB sortieren: home oder away abhängig von HOME
    df["BOOKPROB"] = np.where(df["HOME"] == 1, df["bookprob_home"], df["bookprob_away"])

    # Deskriptive Statistik
    mean_overround = df["overround"].mean()
    sd_overround = df["overround"].std()
    mean_bookprob_home = df["bookprob_home"].mean()

    # relative Häufigkeiten von Ergebnissen
    n_matches = df["MATCH_ID"].nunique()  # statt feste Zahl wie 1510
    percent_home_win = (sum(df["FTR"] == "H") / 2) / n_matches
    percent_draw = (sum(df["FTR"] == "D") / 2) / n_matches
    percent_away_win = (sum(df["FTR"] == "A") / 2) / n_matches

    # Zuschauer-Differenz
    df["DIFFATTEND"] = (df["spectators1"] - df["spectators2"]) / 1000

    # Outcome-Variable: Gewinn, wenn das Team das Spiel gewinnt
    df["outcome_win"] = np.where(
        ((df["HOME"] == 1) & (df["FTHG"] > df["FTAG"])) |
        ((df["HOME"] == 0) & (df["FTHG"] < df["FTAG"])),
        True, False
    )

    stats = {
        "mean_overround": mean_overround,
        "sd_overround": sd_overround,
        "mean_bookprob_home": mean_bookprob_home,
        "percent_home_win": percent_home_win,
        "percent_draw": percent_draw,
        "percent_away_win": percent_away_win,
        "max_diffattend": df["DIFFATTEND"].max()
    }

    return df, stats