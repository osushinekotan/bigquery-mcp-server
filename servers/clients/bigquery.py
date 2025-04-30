from functools import lru_cache

from google.cloud import bigquery

from servers.settings import PROJECT_ID


@lru_cache  # NOTE: 一旦 lru_cache でキャッシュしておく
def init_bigquery_client() -> bigquery.Client:
    """
    Initialize BigQuery client
    """
    return bigquery.Client(project=PROJECT_ID)
