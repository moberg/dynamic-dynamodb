from datetime import datetime, timedelta

class TimeSeriesTable():
    def __init__(self, config, no_scale_time_period = 0, current_time_provider = lambda: datetime.now()):
        self.config = config
        self.no_scale_time_period = no_scale_time_period
        self.current_time_provider = current_time_provider

        self.config = [t.split(",") for t in config.split('|')]

    def is_in_future(self, table):
        for (prefix, time_format) in self.config:
            if table.startswith(prefix):
                table_date = datetime.strptime(table, time_format)

                table_time = table_date
                current_time = self.current_time_provider()
                normalized_time = TimeSeriesTable.normalize_time(current_time, time_format)

                if table_time >= normalized_time and current_time < (table_time + timedelta(seconds=self.no_scale_time_period)):
                    return True

        return False

    @staticmethod
    def normalize_time(t, time_format):
        return datetime.strptime(t.strftime(time_format), time_format)