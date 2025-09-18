import pandas as pd

# Beispiel: 4 CSV-Dateien
datasets = {}
for name in ["01_across_datasource", "02_across_samples", "03_across_time", "04_reproduction"]:
    datasets[name] = pd.read_csv(f"{name}.xls")