from typing import Optional
import pandas as pd
from glob import glob


class EstateTransaction:
    def load_data(self, fields: Optional[str] = None) -> pd.DataFrame:
        # parse fields
        if fields:
            fields = fields.split(",")

        paths = glob("data/*.csv")
        data = pd.concat(
            [pd.read_csv(path, usecols=fields).iloc[1:] for path in paths]
        ).fillna("")
        return data

    def list(
        self, fields: Optional[str] = None, limit: int = 10, offset: int = 0
    ) -> pd.DataFrame:
        return self.load_data(fields=fields).iloc[offset : offset + limit]

    def retrieve(self, pk: int, fields: Optional[str] = None) -> pd.Series:
        return self.load_data(fields=fields).iloc[pk]
