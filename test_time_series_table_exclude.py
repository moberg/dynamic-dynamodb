# -*- coding: utf-8 -*-
""" Testing the Dynamic DynamoDB scaling methods """
import unittest
import logging
import sys

from datetime import datetime
from dynamic_dynamodb.core.timeseriestable import *

logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)


class TestTimeSeriesTable(unittest.TestCase):

    def test_normalize_time(self):
        current_time = datetime(2016, 5, 8, 20, 01)
        self.assertEqual(normalize_time(current_time, "prod_notifications_%Y-%m"), datetime(2016, 5, 1, 0, 0))

    def test_in_the_future(self):
        time_format = "t_:t_%Y-%m"
        current_time = datetime(2016, 5, 8)

        self.assertEqual(False, is_time_series_in_future("t_2016-01", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-02", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-03", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-04", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-05", time_format, current_time))
        self.assertEqual(True, is_time_series_in_future("t_2016-06", time_format, current_time))
        self.assertEqual(True, is_time_series_in_future("t_2016-07", time_format, current_time))

        self.assertEqual(False, is_time_series_in_future("somethine_that_doest_match-01", time_format, current_time))


    def test_in_the_future2(self):
        time_format = "t_:t_%Y-%m-%d_%H"
        current_time = datetime(2016, 2, 1, 22, 15)

        self.assertEqual(False, is_time_series_in_future("t_2016-01-01_01", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-01-01_23", time_format, current_time))
        self.assertEqual(False, is_time_series_in_future("t_2016-02-01_22", time_format, current_time))
        self.assertEqual(True,  is_time_series_in_future("t_2016-03-01_23", time_format, current_time))
        self.assertEqual(True,  is_time_series_in_future("t_2016-03-04_10", time_format, current_time))

        self.assertEqual(False, is_time_series_in_future("somethine_that_doest_match-01", time_format, current_time))

if __name__ == '__main__':
    unittest.main(verbosity=2)
