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


import unittest
import asyncio
import mock


class TestBuildStockFeed(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    @staticmethod
    def get_target_func(func):
        import feeds.bin.build_stock_feed as bsf


        return getattr(bsf, func)

    @staticmethod
    def _mock_config():
        return {'solr': {
                    'thestore': {
                        'host': 'hostname/'
                        }
                    }
                }

    def test_read_file(self):
        func = self.get_target_func('read_file') 
        data = self.loop.run_until_complete(func('tests/unit/data/feeds/bin/test.csv'))
        self.assertEqual(data, "read test\n")
        #klass = self._get_target_klass()(
        #    self._mock_config()['solr']['thestore'])
        #expected = ("hostname/select?q={solr_query}&start={start}"
        #            "&rows={rows}&wt=json")
        #self.assertEqual(klass.select_url, expected)
