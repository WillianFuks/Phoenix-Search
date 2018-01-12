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
import asyncio
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter
sys.path.append('../..')

import aiohttp
import aiofiles
import google.cloud.bigquery as bq
from feeds.connectors.solr import SolrConnector
from feeds.connectors.redshift import RedshiftConnector
from feeds.config import config


async def run(store, rows, max_threads=100):
    loop = asyncio.get_event_loop()
    solr_con = SolrConnector(config['solr'][store])
    red = RedshiftConnector(**config['redshift'][store]['conn'])
    docs, tasks = [], []
    solr_docs_query = solr_con.select_url.format(solr_query="*:*", start=0,
        rows=0)
    red_query = await read_file(config['redshift']['skus_query_path'])
    sem = asyncio.Semaphore(max_threads)
    bq_con = bq.Client.from_service_account_json(config['bigquery']['json_path'])
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        async with aiohttp.ClientSession() as sess:
            total_solr_docs = await get_total_solr_docs(sess, solr_docs_query,
                solr_con)
            with red.con() as red_con:
                for i in range(math.floor(0 / rows) + 1):
                    solr_query = build_solr_query(solr_con, i * rows, rows)
                    tasks.append(asyncio.ensure_future(
                        get_docs(sem, executor, sess, solr_con, red_con,
                            solr_query, red_query, i * rows, rows,
                            **config['redshift'][store])))
                #return [row for e in await asyncio.gather(*tasks) for
                #        row in e]

                return export_data_bq(bq_con,
                     ''.join([row for e in await asyncio.gather(*tasks) for
                         row in e]),
                     **config['bigquery'])


def export_data_bq(bq_con, data, **kwargs):
    """Gets resulting data from Solr and Redshift and uploads to a specified
    table in BigQuery.

    :type bq_con: `google.cloud.bigquery.Client()`
    :param bq_con: client used to interact with BigQuery API.

    :type data: list
    :param data: list with strings from result of merging Solr with Redshift
                 data.

    :param kwargs:
      :type table: str
      :param table: table name where to upload the data.

      :type dataset: str
      :param dataset: dataset where table is located.
    """
    with open('test.csv', 'w') as f:
        f.write(data)
    from google.cloud.bigquery import LoadJobConfig
    print(data)
    ds_ref = bq_con.dataset(kwargs['dataset'])
    ds = bq_con.get_dataset(ds_ref)
    table = ds.table(kwargs['table'])
    conf = LoadJobConfig()
    conf.write_disposition = 'WRITE_TRUNCATE'
    conf.autodetect = True
    conf.source_format = 'NEWLINE_DELIMITED_JSON'
    return bq_con.load_table_from_file(StringIO(data), table,
               job_config=conf).result()


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
        

async def get_docs(sem, executor, session, solr_con, red_con, solr_query,
    red_query, start, rows, **kwargs):
    """Gets docs and their fields. Two main sources are used here: Solr and
    RedShift. The latter is queried using threading so to not block the loop
    with a IO bound request perceived as CPU bound.

    :type sem: `asyncio.Sempahore`
    :param sem: sempahore used to limit total coroutines allowed to run.

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
    async with sem:
        solr_docs = json.loads(await solr_con.query(solr_query, session)).get(
            'response', {}).get('docs', [])
        temp_red_query = red_query.format(indx=int(start / rows),
            skus_list=extract_solr_products(solr_docs), **kwargs)
        loop = asyncio.get_event_loop()
        red_data = {row['sku']: row for e in await asyncio.gather(
            loop.run_in_executor(executor, red_con.query, temp_red_query)) for
            row in e}
        return [str({**doc, **red_data[doc['sku']]}) + '\n' for doc in solr_docs]

def build_final_data(data):
    """Gets resulting data from merging of Solr and Redshift and transform
    to expected output used by front-end team. 

    :type data: dict
    :param data: information for each sku, i.e.,
                     [{'sku': 'sku0', 'url': 'url_0', 'cat': 'cat0'},
                      {'sku': 'sku1', 'url': 'url_1', 'cat': 'cat1'}]

    :rtype: dict 
    :returns: dict with keys and values adapted for front end team.
    """                 
    pass


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
    r = loop.run_until_complete(run('dafiti', 5))
    print(r)
