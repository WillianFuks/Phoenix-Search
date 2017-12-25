#MIT License
#
#Copyright (c) 2017 Willian Fuks
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.


import json
import unittest
import mock
import datetime
import os
import shutil
from collections import Counter

from base import BaseTests


class TestUtils(unittest.TestCase, BaseTests):
    def setUp(self):
        self.prepare_environ()    

    def tearDown(self):
        self.clean_environ()

    @staticmethod
    def load_mock_config():
        data = (open('tests/unit/data/gae/test_config.json')
                .read().replace("config = ", ""))
        return json.loads(data)

    def test_yesterday_date(self):
        expected = datetime.datetime.now() + datetime.timedelta(days=-1)
        result = self.utils.yesterday_date()
        self.assertEqual(expected.date(), result.date())

    @mock.patch('gae.utils.uuid')
    def test_search_query_job_body(self, uuid_mock):
        query_str = ("SELECT 1 FROM `project123.source_dataset.source_table` "
                     "WHERE date={date}")

        expected = {'jobReference': {
                      'projectId': 'project123',
                      'jobId': 'name'
                      },
                  'configuration': {
                      'query': {
                          'destinationTable': {
                              'datasetId': 'dest_dataset',
                              'tableId': 'search_20171010',
                              'projectId': 'dest_project'
                               },
                          'maximumBytesBilled': 100000000000,
                          'query': "",
                          'useLegacySql': False
                          }
                      }
                   }
        uuid_mock.uuid4.return_value = 'name'
        result = self.utils.search_query_job_body(**dict(
            self.load_mock_config()['jobs'][
            'update_dashboard_tables'].items() + [('date', '20171010')]))
        expected['configuration']['query']['query'] = query_str.format(date=
            "20171010")
        print 'EXPECTED ', expected
        print 'RESULT ', result
        self.assertEqual(expected, result)

    def test_format_date(self):
        result = self.utils.format_date("20171010")
        expected = "2017-10-10"
        self.assertEqual(result, expected)

    def test_process_url_date(self):
        expected = None
        result = self.utils.process_url_date(None)
        self.assertEqual(expected, result)

        expected = "20171010"
        result = self.utils.process_url_date("20171010")
        self.assertEqual(expected, result)

        with self.assertRaises(ValueError):
            self.utils.process_url_date("2017-10-10")
