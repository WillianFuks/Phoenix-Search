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
import datetime
import unittest                                                                 
import mock                                                                     
import shutil                                                                   
import os     

import webtest                                                                  


class TestWorkerBase(object):                                                   
    _source_config = 'tests/unit/data/gae/test_config.json'
    _dest_config = 'gae/config.py'
    _remove_config_flag = False
    @classmethod                                                                
    def load_worker_setup(cls):                                                 
        try:                                                                    
            import gae.worker as worker                                
        except ImportError:                                                     
            shutil.copyfile(cls._source_config, cls._dest_config)               
            cls._remove_config_flag = True                                      

        import gae.worker as worker                                    
        from gae import utils                                          
        cls.utils = utils                                                       
        cls.worker = worker                                                     

    @classmethod                                                                
    def clean_config(cls):                                                      
        if cls._remove_config_flag:                                             
            os.remove(cls._dest_config)

                                                                                
class TestWorkerService(unittest.TestCase, TestWorkerBase):                     
    @classmethod                                                                
    def setup_class(cls):                                                       
        cls.load_worker_setup()                                                 
        cls._test_app = webtest.TestApp(cls.worker.app)                         
                                                                                
    @classmethod                                                                
    def teardown_class(cls):                                                    
        cls.clean_config()                                                      
                                                                                
    @classmethod                                                                
    def load_mock_config(cls):                                                  
        return json.loads(open(cls._source_config).read().replace(              
            "config = ", ""))  

    @mock.patch('gae.utils.uuid')                                      
    @mock.patch('gae.worker.gcp_service')
    def test_update_search_tables(self, service_mock, uuid_mock):
        uuid_mock.uuid4.return_value = 'name'
        # this means that the config file is a pre-defined one
        # so we need to replace it in this test
        if not self._remove_config_flag:
            self.worker.config = self.load_mock_config()

        query_job_body = self.utils.search_query_job_body(
            **dict(self.worker.config['jobs'][
                'update_dashboard_tables'].items() +
                [('date', '20171010')]))
                                                                                
        service_mock.bigquery.execute_job.return_value = 'job' 
        response = self._test_app.post("/update_dashboard_tables", {'date':
            "20171010"})

        service_mock.bigquery.execute_job.assert_any_call(*['project123',
            query_job_body]) 
        service_mock.bigquery.poll_job.assert_called_once_with('job')
        self.assertEqual(response.status_int, 200)                              

        dt = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime(
            "%Y%m%d")

        service_mock.bigquery.delete_table.assert_any_call(
            project_id='dest_project', dataset_id='dest_dataset',
            table_id='search_{}'.format(dt))

    @mock.patch('gae.utils.uuid')                                      
    @mock.patch('gae.worker.gcp_service')
    def test_update_search_tables_no_date(self, service_mock, uuid_mock):
        uuid_mock.uuid4.return_value = 'name'
        # this means that the config file is a pre-defined one
        # so we need to replace it in this test
        if not self._remove_config_flag:
            self.worker.config = self.load_mock_config()

        dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
            "%Y%m%d")

        query_job_body = self.utils.search_query_job_body(
            **dict(self.worker.config['jobs'][
                'update_dashboard_tables'].items() +
                [('date', dt)]))
                                                                                
        service_mock.bigquery.execute_job.return_value = 'job' 
        response = self._test_app.post("/update_dashboard_tables")

        service_mock.bigquery.execute_job.assert_any_call(*['project123',
            query_job_body]) 
        service_mock.bigquery.poll_job.assert_called_once_with('job')
        self.assertEqual(response.status_int, 200)                              

        dt = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime(
            "%Y%m%d")

        service_mock.bigquery.delete_table.assert_any_call(
            project_id='dest_project', dataset_id='dest_dataset',
            table_id='search_{}'.format(dt))
