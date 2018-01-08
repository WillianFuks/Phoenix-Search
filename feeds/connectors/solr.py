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


"""Connector to run queries against a Solr DB. The specification for Solr
should be in the file "config.py"""


class SolrConnector(object):
    """Class responsible for running queries against Solr.

    :type config: dict
    :param config: general setup for Solr connection.

      :type config.host: str
      :param config.host: hostname where Solr is hosted. This is where we
                          issue our requests to.
    """
    def __init__(self, config):
        self._config = config 

    @property
    def select_url(self):
        """Builds the URL to issue ``select`` statements against Solr.

        :rtype: url
        :returns: URL to run select queries against Solr.
        """
        return ''.join([self._config['host'],
            'select?q={solr_query}&start={start}&rows={rows}&wt=json'])

    async def query(self, query, session):
        """Runs asynchronously a query against Solr.

        :type query: str
        :param query: query to run against Solr.
    
        :type session: `aiohttp.ClientSession`
        :param session: Client Session to run requests asynchronously.

        :rtype: binary
        :return: Solr response already encoded as UTF-8.
        """
        async with session.get(query) as resp:
            return await resp.read()
