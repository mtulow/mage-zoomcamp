import re
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data: pd.DataFrame, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    
    # Create a function to convert camel case string to snake case 
    def camel_to_snake(text: str) -> str:
        """
        Converts a camel case string to snake case.
        Example: camelToSnakeCase -> camel_to_snake_case
        """
        # Use regex to identify word boundaries and replace them with underscores
        # followed by the lowercase version of the matched word.
        # Additionally, handle consecutive uppercase letters by appending them as lowercase.
        return re.sub(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', '_', text).lower()
    
    # Rename columns in Camel Case to Snake Case
    data.rename(columns={col: camel_to_snake(col) for col in data.columns}, inplace=True)

    # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data.insert(
        list(data.columns).index('lpep_pickup_datetime'),
        'lpep_pickup_date',
        data.lpep_pickup_datetime.dt.date,
    )
    data.insert(
        list(data.columns).index('lpep_dropoff_datetime'),
        'lpep_dropoff_date',
        data.lpep_dropoff_datetime.dt.date,
    )
    
    return data



@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # `vendor_id` is one of the existing values in the column (currently)
    assert 'vendor_id' in list(output.columns), f'`vendor_id` not in columns'
    # `passenger_count` is greater than 0
    zero_passengers = (output.passenger_count == 0).sum()
    assert zero_passengers == 0, f'{zero_passengers} taxi trips in dataframe with zero passengers.'
    # `trip_distance` is greater than 0
    zero_distance = (output.trip_distance == 0).sum()
    assert zero_distance == 0, f'{zero_distance} taxi trips in dataframe with zero passengers.'
