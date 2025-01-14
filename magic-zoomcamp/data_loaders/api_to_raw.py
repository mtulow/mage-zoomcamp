import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    def fetch_data(service: str, year: int, month: int):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
        return pd.read_parquet(url)

    service = kwargs['taxi_service']
    year = kwargs['year']
    if 'months' in kwargs:
        months = kwargs.get('months')
        return pd.concat([fetch_data(service,year,month) for month in months])
    elif 'month' in kwargs:
        month = kwargs['month']
        return fetch_data(service,year,month)
    else:
        months = range(1,12+1)
        return pd.concat([fetch_data(service,year,month) for month in months])

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
