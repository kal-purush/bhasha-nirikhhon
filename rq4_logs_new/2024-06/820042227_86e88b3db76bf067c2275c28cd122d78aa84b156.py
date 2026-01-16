from pathlib import Path
import pandas as pd

file_path = Path(__file__).parent / "penguins.csv"
df = pd.read_csv(file_path)