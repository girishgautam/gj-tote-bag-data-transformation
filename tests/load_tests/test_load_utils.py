from utils.lambda_utils import parquet_to_dataframe
import pytest
import pandas as pd
import io


class TestParquetToDataframe:

    def test_returns_dataframe(self):
        byte_stream = io.BytesIO()
        df = pd.DataFrame.from_dict({"key": ["value"]})
        pqt = df.to_parquet(byte_stream, index=False)
        expected_output = parquet_to_dataframe(byte_stream)
        expected_result = pd.DataFrame.from_dict({"key": ["value"]})
        assert isinstance(expected_result, pd.DataFrame)
        assert expected_output.to_string() == expected_result.to_string()