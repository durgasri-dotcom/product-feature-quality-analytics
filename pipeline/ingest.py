import pandas as pd

def load_raw_data(path: str) -> pd.DataFrame:
    """
    Load raw telemetry data.
    """
    df = pd.read_csv(path)
    return df
