# -------------------------------------------------------------
# *** GENERAL FUNCTIONS TO "PATTERNS" AND "DATAPROC JOBS"
# -------------------------------------------------------------
from google.cloud import bigquery
from google.cloud.datacatalog_v1 import DataCatalogClient, Entry, LookupEntryRequest
from google.cloud.exceptions import NotFound


# -------------------------------------------------------
def get_data_catalog_bigquery(
    dc_client: DataCatalogClient, project_id: str, dataset: str, table_name: str
) -> Entry:

    resource_table_id = f"bigquery.table.`{project_id}`.`{dataset}`.`{table_name}`"
    return dc_client.lookup_entry(
        request=LookupEntryRequest(sql_resource=resource_table_id)
    )


# -------------------------------------------------------------
def check_table_exists(bq_client: bigquery.Client, table_id: str) -> bool:
    try:
        return bool(bq_client.get_table(table_id))
    except NotFound:
        return False
