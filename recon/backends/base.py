# Placeholder for alternate backends (e.g., Polars). Signatures only.
class DataFrameBackend:
    def read_csv(self, *args, **kwargs):
        raise NotImplementedError
    def groupby_agg(self, *args, **kwargs):
        raise NotImplementedError
    def join(self, *args, **kwargs):
        raise NotImplementedError
