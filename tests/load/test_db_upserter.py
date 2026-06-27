from unittest.mock import MagicMock

from src.load.db_upserter import (
    upsert_location_dimension,
    upsert_product_dimension,
    upsert_store_dimension,
)


def test_upsert_product_dimension():
    # Arrange
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (100,)
    values = ("prod_1",)

    # Act
    res = upsert_product_dimension(mock_cur, values)

    # Assert
    assert res == 100
    mock_cur.execute.assert_called_once()
    sql_arg = mock_cur.execute.call_args[0][0]
    assert "INSERT INTO dim_product" in sql_arg
    assert mock_cur.execute.call_args[0][1] == values


def test_upsert_store_dimension():
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (200,)
    values = ("store_1", "Store 1")

    res = upsert_store_dimension(mock_cur, values)

    assert res == 200
    mock_cur.execute.assert_called_once()
    assert "INSERT INTO dim_store" in mock_cur.execute.call_args[0][0]


def test_upsert_location_dimension():
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (300,)
    values = ("loc_1", "United States", "US", "California", "San Jose")

    res = upsert_location_dimension(mock_cur, values)

    assert res == 300
    mock_cur.execute.assert_called_once()
    assert "INSERT INTO dim_location" in mock_cur.execute.call_args[0][0]


def test_upsert_no_return_value():
    # If the upsert doesn't return anything (e.g. no fetchone result)
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = None
    values = ("prod_2",)

    res = upsert_product_dimension(mock_cur, values)
    assert res is None
    mock_cur.execute.assert_called_once()
