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


"""Script to run exportage from Solr and Redshift systems to BigQuery."""


import json
import math
import sys
sys.path.append('../..')

import asyncio
import aiohttp
from feeds.connectors.solr import SolrConnector
from feeds.config import config


async def run(store, rows):
    loop = asyncio.get_event_loop()
    solr_conn = SolrConnector(config['solr'][store])
    docs = []

    async with aiohttp.ClientSession() as session:
        query = solr_conn.select_url.format(solr_query="*:*", start=0,
            rows=0)
        total_docs = await get_total_docs(session, query, solr_conn)
        tasks = []
        for i in range(math.floor(2000 / rows) + 1):
             tasks.append(asyncio.ensure_future(
                         get_docs(session, solr_conn, i * rows, rows)))
        return await asyncio.gather(*tasks)


async def get_total_docs(session, query, solr_conn):
    """Runs a query against Solr and retrieves total docs available. We'll
    use this information later on to iterate over all skus and their fields
    so we can feed our Search Engine.

    :type sess: `aiohttp.ClientSession()`
    :param sess: session used to run http requests asynchronously.

    :type query: str
    :param query: query to run against Solr.

    :type solr_conn: `feeds.connectors.SolrConnection()`
    :param solr_conn: connector used to interact with Solr.

    :rtype: int
    :returns: total documents found in Solr query result.
    """
    return int(json.loads(await solr_conn.query(query, session)).get(
        'response', {}).get('numFound', 0))
        

async def get_docs(session, solr_conn, start, rows):
    """Gets docs and their fields. Two main sources are used here: Solr and
    RedShift. The latter is queried using threading so to not block the loop
    with a IO bound request perceived as CPU bound.

    :type sess: `aiohttp.ClientSession()`
    :param sess: session used to run http requests asynchronously.

    :type solr_conn: `feeds.connectors.SolrConnection()`
    :param solr_conn: connector used to interact with Solr.

    :type start: int
    :param start: this is used to paginate results in Solr, works as an offset.

    :type rows: int
    :param rows: how many rows to retrieve in the request.

    :rtype: list
    :returns: list with skus and their fields description.
    """
    print('getting docs from %s' %(start))
    query = solr_conn.select_url.format(solr_query="*:*", start=start,
        rows=rows) + '&fl=*'
    solr_docs = json.loads(await solr_conn.query(query, session)).get(
        'response', {}).get('docs', [])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    r = loop.run_until_complete(run('dafiti', 1000))    
    
