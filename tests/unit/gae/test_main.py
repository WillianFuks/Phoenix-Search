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


import sys
import os
import mock
import unittest
import json

import webtest
from google.appengine.ext import testbed
from werkzeug.datastructures import ImmutableMultiDict
from base import BaseTests


class TestMainService(unittest.TestCase, BaseTests):
    test_app = None
    def setUp(self):
        self.prepare_environ() 
        from gae.main import app
        self.test_app = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()

    def tearDown(self):
        self.clean_environ()
        self.testbed.deactivate()

    @mock.patch('gae.main.jobs_factory')
    def test_run_job(self, factory_mock):
        import types
        scheduler_mock = mock.Mock()
        def __istr__(self, scheduler_obj):
            return 'OK!'
        scheduler_mock.__str__ = types.MethodType(__istr__, scheduler_mock)
        factory_mock.factor_job.return_value = scheduler_mock
        response = self.test_app.get('/run_job/job_name_test/')
        factory_mock.factor_job.assert_called_once_with(*['job_name_test'])
        scheduler_mock.run.assert_called_once()
        self.assertEqual(response.status_int, 200)

        url = '/run_job/job_name_test/?date=xxxx'
        response = self.test_app.get(url)
        expected = ImmutableMultiDict([('date', 'xxxx')])
        scheduler_mock.run.assert_called_with(expected)
        self.assertEqual(response.status_int, 200)

        self.assertEqual(response.text, "OK!")
