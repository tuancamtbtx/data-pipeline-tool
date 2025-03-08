from datetime import datetime, timedelta

class DateMacro:
    def __init__(self):
        # Initialize with the current date
        self.current_date = datetime.now()

    def today(self, format_string="%Y-%m-%d"):
        """Return today's date as a string in the specified format."""
        return self.current_date.strftime(format_string)

    def yesterday(self, format_string="%Y-%m-%d"):
        """Return yesterday's date as a string in the specified format."""
        yesterday_date = self.current_date - timedelta(days=1)
        return yesterday_date.strftime(format_string)

    def tomorrow(self, format_string="%Y-%m-%d"):
        """Return tomorrow's date as a string in the specified format."""
        tomorrow_date = self.current_date + timedelta(days=1)
        return tomorrow_date.strftime(format_string)

    def days_ago(self, days, format_string="%Y-%m-%d"):
        """Return the date 'days' days ago as a string in the specified format."""
        target_date = self.current_date - timedelta(days=days)
        return target_date.strftime(format_string)

    def days_from_now(self, days, format_string="%Y-%m-%d"):
        """Return the date 'days' days from now as a string in the specified format."""
        target_date = self.current_date + timedelta(days=days)
        return target_date.strftime(format_string)

    def custom_date(self, year, month, day, format_string="%Y-%m-%d"):
        """Return a custom date as a string in the specified format."""
        custom_date = datetime(year, month, day)
        return custom_date.strftime(format_string)
