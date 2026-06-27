import hashlib

from src.processing.data_transformer import (
    browser_transformer,
    customer_transformer,
    date_transformer,
    device_transformer,
    os_transformer,
    parse_user_agent,
    store_transformer,
)


def test_store_transformer():
    assert store_transformer("123") == "Store 123"
    assert store_transformer("A") == "Store A"


def test_customer_transformer():
    # Test valid data
    res = customer_transformer("cust_1", "test@test.com", "db_1")
    assert res == {
        "customer_id": "cust_1",
        "email_address": "test@test.com",
        "user_id_db": "db_1",
    }

    # Test missing/empty data
    res = customer_transformer("", "", "")
    assert res == {
        "customer_id": "-1",
        "email_address": "Not Defined",
        "user_id_db": "Not Defined",
    }

    res = customer_transformer(None, None, None)
    assert res == {
        "customer_id": "-1",
        "email_address": "Not Defined",
        "user_id_db": "Not Defined",
    }


def test_device_transformer():
    # Test valid inputs
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    res_val = "1920x1080"
    res = device_transformer(ua, res_val)

    expected_device_string = f"{ua}_{res_val}"
    expected_id = hashlib.sha256(expected_device_string.encode("utf-8")).hexdigest()

    assert res["device_id"] == expected_id
    assert res["user_agent"] == ua
    assert res["resolution"] == res_val

    # Test empty/missing inputs
    res_empty = device_transformer("", "")
    assert res_empty["user_agent"] == "Not Defined"
    assert res_empty["resolution"] == "Not Defined"

    expected_device_string_empty = "Not Defined_Not Defined"
    expected_id_empty = hashlib.sha256(
        expected_device_string_empty.encode("utf-8")
    ).hexdigest()
    assert res_empty["device_id"] == expected_id_empty


def test_date_transformer_unix():
    # Use a known timestamp: Jan 1, 2023, 12:00:00 PM UTC (1672574400)
    ts = 1672574400
    res = date_transformer(ts)

    assert res["date_id"] == 20230101
    assert str(res["full_date"]) == "2023-01-01"
    assert res["date_of_week"] == "Sunday"
    assert res["date_of_week_short"] == "Sun"
    assert res["is_weekday_or_weekend"] == "weekend"
    assert res["day_of_month"] == 1
    assert res["day_of_year"] == 1
    assert res["week_of_year"] == 52  # 2023-01-01 was the end of the last week of 2022
    assert res["quarter_number"] == 1
    assert res["year_number"] == 2023
    assert res["year_month"] == "202301"


def test_date_transformer_unix_ms():
    # Millisecond timestamp for the same time
    ts_ms = 1672574400000
    res = date_transformer(ts_ms)
    assert res["date_id"] == 20230101


def test_date_transformer_iso():
    # ISO string
    ts_iso = "2023-01-01T12:00:00+00:00"
    res = date_transformer(ts_iso)
    assert res["date_id"] == 20230101


def test_date_transformer_invalid():
    assert date_transformer(None) is None
    assert date_transformer("") is None
    assert date_transformer("invalid-date") is None


def test_parse_user_agent():
    ua_string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    browser, os = parse_user_agent(ua_string)
    assert browser == "Chrome"
    assert os == "Windows"

    # Test empty
    assert parse_user_agent(None) == ("Unknown", "Unknown")


def test_browser_transformer():
    ua_string = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    assert browser_transformer(ua_string) == "Mobile Safari"


def test_os_transformer():
    ua_string = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    assert os_transformer(ua_string) == "iOS"
