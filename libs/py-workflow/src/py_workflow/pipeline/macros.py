from typing import Union
import datetime


def ds_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Date filter."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%d")


def ds_nodash_filter(
    value: Union[datetime.date, datetime.time, None],
) -> Union[str, None]:
    """Date filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%d")


def ts_filter(value: Union[datetime.date, datetime.time, None]) -> Union[str, None]:
    """Timestamp filter."""
    if value is None:
        return None
    return value.isoformat()


def ts_nodash_filter(
    value: Union[datetime.date, datetime.time, None],
) -> Union[str, None]:
    """Timestamp filter without dashes."""
    if value is None:
        return None
    return value.strftime("%Y%m%dT%H%M%S")


def ts_nodash_with_tz_filter(
    value: Union[datetime.date, datetime.time, None],
) -> Union[str, None]:
    """Timestamp filter with timezone."""
    if value is None:
        return None
    return value.isoformat().replace("-", "").replace(":", "")


def previous_month_filter(date):
    first_day_of_current_month = date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y%m")


def start_month_filter(date):
    start_of_month = date.replace(day=1)
    return start_of_month.strftime("%Y-%m-%d")


def year_month_filter(data):
    return data.strftime("%Y%m")
