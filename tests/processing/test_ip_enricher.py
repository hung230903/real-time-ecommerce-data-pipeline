import hashlib
from unittest.mock import MagicMock, patch

from src.processing.ip_enricher import get_loc_info


@patch("src.processing.ip_enricher.os.path.exists")
@patch("src.processing.ip_enricher.IP2Location.IP2Location")
def test_get_loc_info_success(mock_ip2location, mock_exists):
    # Mock file existence
    mock_exists.return_value = True

    # Mock IP2Location object and its get_all method
    mock_db_instance = MagicMock()
    mock_ip2location.return_value = mock_db_instance

    mock_info = MagicMock()
    mock_info.country_long = "United States"
    mock_info.country_short = "US"
    mock_info.region = "California"
    mock_info.city = "San Jose"
    mock_db_instance.get_all.return_value = mock_info

    # Run the function
    result = get_loc_info("8.8.8.8", "dummy_path.BIN")

    # Validate the results
    assert result is not None
    loc_id, country_long, country_short, region, city = result

    assert country_long == "United States"
    assert country_short == "US"
    assert region == "California"
    assert city == "San Jose"

    expected_loc_string = "United States|California|San Jose"
    expected_loc_id = hashlib.md5(expected_loc_string.encode("utf-8")).hexdigest()
    assert loc_id == expected_loc_id

    # Reset singleton state for other tests if necessary
    import src.processing.ip_enricher as ip_enricher

    ip_enricher._ip2loc_instance = None
    ip_enricher._ip2loc_loaded_path = None


@patch("src.processing.ip_enricher.os.path.exists")
def test_get_loc_info_db_not_found(mock_exists):
    # Mock that neither the full path nor the filename exists
    mock_exists.return_value = False

    result = get_loc_info("8.8.8.8", "missing_path.BIN")
    assert result is None


@patch("src.processing.ip_enricher.os.path.exists")
@patch("src.processing.ip_enricher.IP2Location.IP2Location")
def test_get_loc_info_exception(mock_ip2location, mock_exists):
    mock_exists.return_value = True

    mock_db_instance = MagicMock()
    mock_ip2location.return_value = mock_db_instance
    # Simulate an exception in get_all
    mock_db_instance.get_all.side_effect = Exception("Test Exception")

    result = get_loc_info("8.8.8.8", "dummy_path.BIN")
    assert result is None

    import src.processing.ip_enricher as ip_enricher

    ip_enricher._ip2loc_instance = None
    ip_enricher._ip2loc_loaded_path = None
