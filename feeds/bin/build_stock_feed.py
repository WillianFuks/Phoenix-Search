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
import os
import concurrent.futures
import asyncio
sys.path.append('../..')

import aiohttp
import aiofiles
from feeds.connectors.solr import SolrConnector
from feeds.connectors.redshift import RedshiftConnector
from feeds.config import config


async def run(store, rows, max_threads=100):
    loop = asyncio.get_event_loop()
    solr_con = SolrConnector(config['solr'][store])
    red_con = RedshiftConnector(**config['redshift'][store]['conn'])
    docs, tasks = [], []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_threads) as executor:
        async with aiohttp.ClientSession() as session:
            query = solr_con.select_url.format(
                solr_query="*:*", start=0, rows=0)
            total_docs = await get_total_solr_docs(session, query, solr_con)
            red_query = await read_file(config['redshift']['skus_query_path'])
            for i in range(math.floor(2 / rows) + 1):
                print("processing step :", i)
                solr_query = build_solr_query(solr_con, i * rows, rows)
                tasks.append(asyncio.ensure_future(
                    get_docs(executor, session, solr_con, red_con,
                             solr_query, red_query, i * rows, rows, 
                             **config['redshift'][store])))
            return await asyncio.gather(*tasks)

def build_solr_query(solr_con, start, rows):
    """Builds select URL to retrieve data from Solr.

    :type solr_con: `connectors.solr.SolrConnector()`
    :param solr_con: connector used to interact with Solr.

    :type start: int
    :param start: offset used to query Solr.

    :type rows: int
    :param rows: how many rows to retrieve from Solr.

    :rtype: str
    :returns: desired query to retrieve all products from Solr and the selected
              fields.
    """
    return solr_con.select_url.format(solr_query="*:*", start=start,
        rows=rows) + '&fl=*'


async def get_total_solr_docs(session, query, solr_con):
    """Runs a query against Solr and retrieves total docs available. We'll
    use this information later on to iterate over all skus and their fields
    so we can feed our Search Engine.

    :type sess: `aiohttp.ClientSession()`
    :param sess: session used to run http requests asynchronously.

    :type query: str
    :param query: query to run against Solr.

    :type solr_con: `feeds.connectors.SolrConnection()`
    :param solr_con: connector used to interact with Solr.

    :rtype: int
    :returns: total documents found in Solr query result.
    """
    return int(json.loads(await solr_con.query(query, session)).get(
        'response', {}).get('numFound', 0))
        

async def get_docs(executor, session, solr_con, red_con, solr_query, red_query,
    start, rows, **kwargs):
    """Gets docs and their fields. Two main sources are used here: Solr and
    RedShift. The latter is queried using threading so to not block the loop
    with a IO bound request perceived as CPU bound.

    :type executor: `concurrent.futures.ThreadPoolExecutor()`
    :param executor: executor to run thread operations.

    :type sess: `aiohttp.ClientSession()`
    :param sess: session used to run http requests asynchronously.

    :type solr_con: `feeds.connectors.SolrConnection()`
    :param solr_con: connector used to interact with Solr.

    :type start: int
    :param start: this is used to paginate results in Solr, works as an offset.

    :type rows: int
    :param rows: how many rows to retrieve in the request.

    :param kwargs:
      :type kwargs.fk_company: int
      :param kwargs.fk_company: company to filter on when running redshift
                                query.

    :rtype: list
    :returns: list with skus and their fields description.
    """
    print('getting docs from %s' %(start))
    solr_docs = json.loads(await solr_con.query(solr_query, session)).get(
        'response', {}).get('docs', [])
    temp_red_query = red_query.format(indx=int(start / rows),
        skus_list=extract_solr_products(solr_docs), **kwargs)
    #print("value of query: ", temp_red_query)
    loop = asyncio.get_event_loop()
    resp = await asyncio.gather(loop.run_in_executor(
        executor, red_con.query, temp_red_query))
    return resp


def extract_solr_products(solr_docs):
    """Retrieves all products available in Solr response.

    :type solr_docs: dict
    :param solr_docs: contains all data from Solr response in the format
                      [{'sku': 'sku0', 'category': 1},
                       {'sku': 'sku1', 'category': 2}]

    :rtype: str
    :returns: all products from Solr in the format "('sku0'), ('sku1')" to be
              used later on for Redshift query filter.
    """
    return ','.join([''.join(["('", e['sku'], "')"]) for e in solr_docs])


async def read_file(path):
    """Reads SQL from a file. It does so asynchronously so we can already issue
    other concurrent operations that does not require the SQL data in order to
    execute.

    :type path: str
    :param path: path from where to read file.

    :rtype: str
    :returns: data from file. 
    """
    async with aiofiles.open(path) as f:
        return await f.read()

 
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    r = loop.run_until_complete(run('dafiti', 2))
    print(len(r))
