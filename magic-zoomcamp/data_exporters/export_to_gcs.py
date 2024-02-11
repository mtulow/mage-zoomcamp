from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    service = kwargs['taxi_service']
    year = kwargs['year']
    if 'months' in kwargs:
        months = kwargs.get('months', range(1,12+1))
        file_name = f'{service}_tripdata_{year}_{min(months):02d}-{max(months):02d}.parquet'
    elif 'month' in kwargs:
        month = kwargs['month']
        file_name = f'{service}_tripdata_{year}_{month:02d}.parquet'
    else:
        raise ValueError

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'nyc-tlc_taxi-trip'
    object_key = file_name
    
    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
