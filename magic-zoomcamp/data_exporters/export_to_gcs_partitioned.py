import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/mage-sa.json'

project_id = 'dtc-de-mst-zoomcamp-2024'
bucket_name = 'nyc-tlc_taxi-trip'


@data_exporter
def export_data(df: pd.DataFrame, **kwargs) -> None:
    # 
    service = kwargs['taxi_service']
    # 
    table_name = f'{service}_taxi'
    root_path = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['pickup_date'],
        filesystem=gcs
    )