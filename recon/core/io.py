from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

@dataclass
class ReadSpec:
    path: str
    delimiter: str = ','
    encoding: str = 'utf-8'
    dtypes: dict | None = None
    header: int | None = 0

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.path, delimiter=self.delimiter, encoding=self.encoding, header=self.header)
        if self.dtypes:
            for k, v in self.dtypes.items():
                if v.startswith('date'):
                    df[k] = pd.to_datetime(df[k], errors='coerce')
                else:
                    df[k] = df[k].astype(v, errors='ignore')
        return df
