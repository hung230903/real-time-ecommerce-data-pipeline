import hashlib
import logging
import os

import IP2Location

# Lazy-loaded singleton: the IP2Location database is loaded once per worker
# process and reused for all subsequent rows, avoiding the massive overhead
# of re-reading the ~98 MB .BIN file for every single IP lookup.
_ip2loc_instance = None
_ip2loc_loaded_path = None


def _get_ip2loc(db_path):
    """Return a cached IP2Location instance, loading it only on first call."""
    global _ip2loc_instance, _ip2loc_loaded_path

    actual_path = db_path
    if not os.path.exists(actual_path):
        # If not, check if the file exists in the current directory (where Spark addFile puts it)
        filename = os.path.basename(db_path)
        if os.path.exists(filename):
            actual_path = filename
        else:
            logging.warning(f"IP2LOC_DB NOT FOUND at {db_path} or {filename}")
            return None

    if _ip2loc_instance is None or _ip2loc_loaded_path != actual_path:
        _ip2loc_instance = IP2Location.IP2Location(actual_path)
        _ip2loc_loaded_path = actual_path

    return _ip2loc_instance


def get_loc_info(ip_address, db_path):
    try:
        ipdb = _get_ip2loc(db_path)
        if ipdb is None:
            return None

        info = ipdb.get_all(ip_address)

        if info:
            country = info.country_long if info.country_long else ""
            region = info.region if info.region else ""
            city = info.city if info.city else ""

            loc_string = f"{country}|{region}|{city}"
            loc_id = hashlib.md5(loc_string.encode("utf-8")).hexdigest()

            return (
                loc_id,
                info.country_long,
                info.country_short,
                info.region,
                info.city,
            )

    except Exception:
        logging.error(ip_address, "IS NOT FOUND")
        return None
    return None
