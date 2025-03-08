from datetime import datetime, timedelta

def ds_add(ds: str, days: int) -> str:
    """
    Add or subtract days from a YYYY-MM-DD.

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :param days: number of days to add to the ds, you can use negative values

    >>> ds_add("2015-01-01", 5)
    '2015-01-06'
    >>> ds_add("2015-01-06", -5)
    '2015-01-01'
    """
    if not days:
        return str(ds)
    dt = datetime.strptime(str(ds), "%Y-%m-%d") + timedelta(days=days)
    return dt.strftime("%Y-%m-%d")

