from user_agents import parse
from datetime import datetime
import hashlib


def store_transformer(store_id):
    store_name = "Store " + store_id
    return store_name


def customer_transformer(customer_id, email_address, user_id_db):
    return {
        'customer_id': customer_id if customer_id else '-1',
        'email_address': email_address if email_address and email_address.strip() else 'Not Defined',
        'user_id_db': user_id_db if user_id_db and user_id_db.strip() else 'Not Defined'
    }


def device_transformer(user_agent, resolution):
    ua_safe = user_agent if user_agent and user_agent.strip() else 'Not Defined'
    res_safe = resolution if resolution and resolution.strip() else 'Not Defined'
    device_string = f"{ua_safe}_{res_safe}"
    device_id = hashlib.sha256(device_string.encode('utf-8')).hexdigest()
    return {
        'device_id': device_id,
        'user_agent': ua_safe,
        'resolution': res_safe
    }


def date_transformer(time_stamp):
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

    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_abbr = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    return {
        'date_id': int(time_stamp.strftime('%Y%m%d')),
        'full_date': time_stamp.date(),
        'date_of_week': day_names[day_of_week_num],
        'date_of_week_short': day_names_abbr[day_of_week_num],
        'is_weekday_or_weekend': 'weekend' if is_weekend else 'weekday',
        'day_of_month': time_stamp.day,
        'day_of_year': day_of_year,
        'week_of_year': week_of_year,
        'quarter_number': quarter_number,
        'year_number': time_stamp.year,
        'year_month': time_stamp.strftime('%Y%m')
    }


def parse_user_agent(ua_string):
    """Parse User-Agent once and return (browser_family, os_family).

    This avoids the cost of parsing the same UA string twice when both
    browser and OS information are needed.
    """
    if not ua_string:
        return "Unknown", "Unknown"
    parsed = parse(ua_string)
    return parsed.browser.family, parsed.os.family


def browser_transformer(browser):
    """Return browser family. Delegates to parse_user_agent."""
    browser_name, _ = parse_user_agent(browser)
    return browser_name


def os_transformer(os):
    """Return OS family. Delegates to parse_user_agent."""
    _, os_name = parse_user_agent(os)
    return os_name
