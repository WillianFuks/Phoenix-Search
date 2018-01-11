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


"""Connector to run queries against a Redshift DB. The specification for the 
RedShift setup should be in the file "config.py located ate root of this
project."""


import contextlib

import psycopg2


class RedshiftConnector(object):
    """Class responsible for running queries against Redshift.

    :type host: str
    :param host: Host where RedShift is located.

    :type dbname: str
    :param dbname: name of database to connect to.

    :type port: int
    :param port: port used to establish connection.

    :type user: str
    :param user: user responsible for the session.

    :type password: str
    :param password: password to establish connection.
    """
    def __init__(self, **kwargs):
        self._con_kwargs = kwargs
        self._con = None

    def query(self, query):
        """Runs `query` against RedShift.

        :type query: str
        :param query: query to run.

        :rtype: list of dicts 
        :returns: list  with results from Redshift operation where each key
                  is a name of a column in result set and value is
                  correspondent value. Returns empyt list if result set is
                  empty.
        """
        with self.cursor() as c:
            c.execute(query)
            return [dict(zip([e.name for e in c.description], row)) for row in 
                    c.fetchall()]  if c.arraysize else []

    @contextlib.contextmanager
    def con(self):
        if not self._con:
            self._con = psycopg2.connect(**self._con_kwargs)
        yield self
        self._con.close()
        self._con = None
        
    @contextlib.contextmanager
    def cursor(self):
        if not self._con:
            raise ValueError(
                'Please first initiate the connection to Redshift.')
        cur = self._con.cursor()        
        yield cur 
        cur.close()
