# -*- coding: utf-8 -*-
""" Testing the Dynamic DynamoDB scaling methods """
import unittest
import logging
import sys

from datetime import datetime
from dynamic_dynamodb.core.timeseriestable import TimeSeriesTable

logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)


class TestTimeSeriesTable(unittest.TestCase):

    def test_normalize_time(self):
        current_time = datetime(2016, 5, 8, 20, 01)
        self.assertEqual(TimeSeriesTable.normalize_time(current_time, "prod_notifications_%Y-%m"), datetime(2016, 5, 1, 0, 0))

    def test_in_the_future3(self):
        config = "t_:t_%Y-%m,t2:t2_%Y-%m"
        current_time = datetime(2016, 5, 8)
        ts = TimeSeriesTable(config, current_time_provider = lambda: current_time)

        self.assertEqual(False, ts.is_in_future("t_2016-01"))
        self.assertEqual(False, ts.is_in_future("t_2016-02"))
        self.assertEqual(False, ts.is_in_future("t_2016-03"))
        self.assertEqual(False, ts.is_in_future("t_2016-04"))
        self.assertEqual(False, ts.is_in_future("t_2016-05"))
        self.assertEqual(True, ts.is_in_future("t_2016-06"))
        self.assertEqual(True, ts.is_in_future("t_2016-07"))

        self.assertEqual(False, ts.is_in_future("t2_2016-02"))
        self.assertEqual(True, ts.is_in_future("t2_2016-07"))

        self.assertEqual(False, ts.is_in_future("somethine_that_doest_match-01"))

    def test_in_the_future2(self):
        config = "t_:t_%Y-%m-%d_%H"
        current_time = datetime(2016, 2, 1, 22, 15)
        ts = TimeSeriesTable(config, current_time_provider = lambda: current_time)

        self.assertEqual(False, ts.is_in_future("t_2016-01-01_01"))
        self.assertEqual(False, ts.is_in_future("t_2016-01-01_23"))
        self.assertEqual(False, ts.is_in_future("t_2016-02-01_22"))
        self.assertEqual(True,  ts.is_in_future("t_2016-03-01_23"))
        self.assertEqual(True,  ts.is_in_future("t_2016-03-04_10"))

        self.assertEqual(False, ts.is_in_future("somethine_that_doest_match-01"))

    def test_no_scale_period(self):
        config = "t_:t_%Y-%m,t2:t2_%Y-%m"

        self.ts_time_test(config, datetime(2016, 6, 30, 23, 59), "t_2016-07", True)
        self.ts_time_test(config, datetime(2016, 7, 1, 0, 0), "t_2016-07", True)
        self.ts_time_test(config, datetime(2016, 7, 1, 0, 4), "t_2016-07", True)
        self.ts_time_test(config, datetime(2016, 7, 1, 0, 5), "t_2016-07", False)
        self.ts_time_test(config, datetime(2016, 7, 1, 0, 7), "t_2016-07", False)
        self.ts_time_test(config, datetime(2016, 7, 1, 0, 1), "t_2016-07", True)
        self.ts_time_test(config, datetime(2016, 7, 2, 0, 6), "t_2016-07", False)

    def ts_time_test(self, config, current_time, table, expected):
        ts = TimeSeriesTable(config, 300, lambda: current_time)
        self.assertEqual(expected, ts.is_in_future(table))


if __name__ == '__main__':
    unittest.main(verbosity=2)
