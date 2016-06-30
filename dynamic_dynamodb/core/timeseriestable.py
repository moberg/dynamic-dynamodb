from datetime import datetime

def is_time_series_in_future(table, settings, current_time):
    time_series_tables = [t.split(":") for t in settings.split(',')]

    for (prefix, time_format) in time_series_tables:
        if table.startswith(prefix):
            table_date = datetime.strptime(table, time_format)

            if table_date > normalize_time(current_time, time_format):
                return True

    return False

def normalize_time(t, time_format):
    return datetime.strptime(t.strftime(time_format), time_format)