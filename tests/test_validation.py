import pytest
from unittest.mock import Mock

from spark_jobs.bronze_to_silver_weather import validate_row_count


def test_validate_row_count_returns_count_when_rows_exist():
    df = Mock()
    df.count.return_value = 5

    result = validate_row_count(df)

    assert result == 5


def test_validate_row_count_raises_error_when_empty():
    df = Mock()
    df.count.return_value = 0

    with pytest.raises(ValueError):
        validate_row_count(df)