import hashlib
from datetime import datetime

from user_agents import parse


def store_transformer(store_id):
    """
    Transforms a raw store ID into a formatted store name.

    Args:
        store_id (str): The original identifier of the store.

    Returns:
        str: A formatted string representing the store name (e.g., "Store 1").
    """
    store_name = "Store " + store_id
    return store_name


def customer_transformer(customer_id, email_address, user_id_db):
    """
    Normalizes customer data, handling missing or empty values.

    Args:
        customer_id (str): The unique identifier for the customer.
        email_address (str): The customer's email address.
        user_id_db (str): The user's database identifier.

    Returns:
        dict: A dictionary containing the normalized customer information with
              default values applied for missing data ("-1" for id, "Not Defined" for others).
    """
    return {
        "customer_id": customer_id if customer_id else "-1",
        "email_address": email_address
        if email_address and email_address.strip()
        else "Not Defined",
        "user_id_db": user_id_db
        if user_id_db and user_id_db.strip()
        else "Not Defined",
    }


def device_transformer(user_agent, resolution):
    """
    Generates a unique device ID based on user agent and screen resolution.

    Args:
        user_agent (str): The User-Agent string from the client's browser/device.
        resolution (str): The screen resolution of the client's device.

    Returns:
        dict: A dictionary containing the generated 'device_id' (SHA-256 hash),
              along with the normalized 'user_agent' and 'resolution' strings.
    """
    ua_safe = user_agent if user_agent and user_agent.strip() else "Not Defined"
    res_safe = resolution if resolution and resolution.strip() else "Not Defined"
    device_string = f"{ua_safe}_{res_safe}"
    device_id = hashlib.sha256(device_string.encode("utf-8")).hexdigest()
    return {"device_id": device_id, "user_agent": ua_safe, "resolution": res_safe}


def date_transformer(time_stamp):
    """
    Transforms a timestamp into a comprehensive set of date-related dimension attributes.

    This function handles both Unix timestamps (in seconds or milliseconds) and ISO format
    date strings. It extracts various calendar components useful for time-series analysis
    and data warehousing (e.g., day of week, quarter, weekend indicator).

    Args:
        time_stamp (int, float, or str): The raw timestamp to process. Can be a Unix
                                         timestamp or an ISO format date string.

    Returns:
        dict: A dictionary containing various date attributes (date_id, full_date,
              day_of_week, etc.), or None if the input timestamp is missing/invalid.
    """
    if not time_stamp:
        return None

    if isinstance(time_stamp, (int, float)):
        from datetime import timezone

        if time_stamp > 9999999999:
            time_stamp = datetime.fromtimestamp(time_stamp / 1000.0, tz=timezone.utc)
        else:
            time_stamp = datetime.fromtimestamp(time_stamp, tz=timezone.utc)

    elif isinstance(time_stamp, str):
        try:
            time_stamp = datetime.fromisoformat(time_stamp)
        except ValueError:
            return None

    day_of_week_num = time_stamp.weekday()
    is_weekend = day_of_week_num >= 5
    day_of_year = time_stamp.timetuple().tm_yday
    week_of_year = time_stamp.isocalendar()[1]
    quarter_number = (time_stamp.month - 1) // 3 + 1

    day_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    day_names_abbr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    return {
        "date_id": int(time_stamp.strftime("%Y%m%d")),
        "full_date": time_stamp.date(),
        "date_of_week": day_names[day_of_week_num],
        "date_of_week_short": day_names_abbr[day_of_week_num],
        "is_weekday_or_weekend": "weekend" if is_weekend else "weekday",
        "day_of_month": time_stamp.day,
        "day_of_year": day_of_year,
        "week_of_year": week_of_year,
        "quarter_number": quarter_number,
        "year_number": time_stamp.year,
        "year_month": time_stamp.strftime("%Y%m"),
    }


def parse_user_agent(ua_string):
    """
    Parses a User-Agent string to extract browser and operating system families.

    This avoids the cost of parsing the same UA string twice when both
    browser and OS information are needed. It leverages the `user_agents` library.

    Args:
        ua_string (str): The raw User-Agent string.

    Returns:
        tuple: A tuple containing (browser_family, os_family). Defaults to
               ("Unknown", "Unknown") if the string is empty or invalid.
    """
    if not ua_string:
        return "Unknown", "Unknown"
    parsed = parse(ua_string)
    return parsed.browser.family, parsed.os.family


def browser_transformer(browser):
    """
    Extracts the browser family from a User-Agent string.

    Args:
        browser (str): The User-Agent string.

    Returns:
        str: The extracted browser family name (e.g., "Chrome", "Firefox").
    """
    browser_name, _ = parse_user_agent(browser)
    return browser_name


def os_transformer(os):
    """
    Extracts the operating system family from a User-Agent string.

    Args:
        os (str): The User-Agent string.

    Returns:
        str: The extracted operating system family name (e.g., "Windows", "iOS").
    """
    _, os_name = parse_user_agent(os)
    return os_name
