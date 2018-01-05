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

    :type store: str
    :param store: which store from where get settings. Defaults to 'dafiti'.

    :type config: dict
    :param config: general setup for Solr connection.

      :type config.host: str
      :param config.host: hostname where Solr is hosted. This is where we
                          issue our requests to.
    """
    def __init__(self, config, store='dafiti'):
        self.select_url = self._build_query_url(config, store) 

    @staticmethod
    def _build_query_url(config, store):
        """Builds the URL to issue ``select`` statements against Solr.

        :type store: str
        :param store: which store from where to run queries from.

        :type config: dict
        :param config: setup to config Solr Connector.

        :rtype: url
        :returns: URL to run select queries against Solr.
        """
        return ''.join([config['solr'][store]['host'], 'select?q={}&wt=json'])

    def get_total_docs(self, select_statement):
        """Returns the total amount of docs available for a given query.

        :type select_statement: str
        :param select_statement: select to run against Solr. It can be
                                 something like "*:*" where all docs are
                                 selected or have some other operation
                                 such as '*:*&fq=brand:brandX'. 

        :rtype: int
        :returns: total docs in result 
        """
        

