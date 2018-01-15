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
from io import BytesIO
import gzip
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter
sys.path.append('../..')

import aiohttp
import aiofiles
import google.cloud.bigquery as bq
import google.cloud.storage as st
from feeds.connectors.solr import SolrConnector
from feeds.connectors.redshift import RedshiftConnector
from feeds.config import config
import time


async def run(store, rows, max_threads=15):
    """Main method to orchestrate all data retrieval from Solr and Redshift
    and its storage to the cloud.

    :type store: str
    :param store: which store at GFG to run this feeder, such as ``dafiti``,
                  ``kanui`` and so on.

    :type rows: int
    :param rows: how many rows to retrieve from each request to Solr.

    :type max_threads: int
    :param max_threads: max allowed threads to run at the same time. This is
                        used to avoid memory swapping.
    """
    loop = asyncio.get_event_loop()
    solr_con = SolrConnector(config['solr'][store])
    red = RedshiftConnector(**config['redshift'][store]['conn'])
    docs, tasks = [], []
    solr_docs_query = solr_con.select_url.format(solr_query="*:*", start=0,
        rows=0)
    red_query = await read_file(config['redshift']['skus_query_path'])
    sem = asyncio.Semaphore(max_threads)
    bq_con = bq.Client.from_service_account_json(config['gcp']['json_path'])
    st_con = st.Client.from_service_account_json(config['gcp']['json_path'])
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        async with aiohttp.ClientSession() as sess:
            total_solr_docs = await get_total_solr_docs(sess, solr_docs_query,
                solr_con)
            with red.con() as red_con:
                for i in range(math.floor(total_solr_docs / rows) + 1):
                    t0 = time.time()
                    solr_query = build_solr_query(solr_con, i * rows, rows)
                    tasks.append(asyncio.ensure_future(
                        get_docs(sem, executor, sess, solr_con, red_con,
                            solr_query, red_query, i * rows, rows)))
                return export_data_bq(bq_con, st_con,
                     ''.join([row for e in await asyncio.gather(*tasks) for
                         row in e]),
                     **{**config['gcp']['bigquery'],
                         **config['gcp']['storage']})


def export_data_bq(bq_con, st_con, data, **kwargs):
    """Gets resulting data from Solr and Redshift and uploads to a specified
    table in BigQuery.

    :type bq_con: `google.cloud.bigquery.Client()`
    :param bq_con: client used to interact with BigQuery API.

    :type st_con: `google.cloud.storage.Client()`
    :param st_con: client used to interact with Storage API.

    :type data: str 
    :param data: string from result of merging Solr with Redshift
                 data.

    :param kwargs:
      :type table: str
      :param table: table name where to upload the data.

      :type dataset: str
      :param dataset: dataset where table is located.
    """
    table = bq_con.dataset(kwargs['dataset']).table(kwargs['table'])
    conf = bq.LoadJobConfig()
    conf.write_disposition = 'WRITE_TRUNCATE'
    conf.autodetect = True
    conf.source_format = 'NEWLINE_DELIMITED_JSON'
    st_con.bucket(kwargs['bucket']).blob(kwargs['blob']).upload_from_file(
        BytesIO(gzip.compress(data.encode('utf-8'))))
    return bq_con.load_table_from_uri("gs://{bucket}/{blob}".format(**kwargs),
            table, job_config=conf).result()


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
        rows=rows) + ('&fl=sku,skus,score_top_seller,suggest_terms,' +
        'suggestions,facet_seller,facet_price,score_novelty,fulltext,' +
        'facet_product_type,score_simple_availability,brand,brand_sort,' +
        'didyoumean,name_autocomplete,facet_gender,facet_color_family,' +
        'facet_category,facet_quality,text_attribute,' +
        'non_text_attribute,facet_discount,activated_at,facet_size,' +
        'facet_brand,score_availability,name,name_sort,score_random,' +
        'category')


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
        

async def get_docs(sem,
                   executor,
                   session,
                   solr_con,
                   red_con,
                   solr_query,
                   red_query,
                   start,
                   rows):
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

    :rtype: list
    :returns: list with skus and their fields description.
    """
    async with sem:
        solr_docs = json.loads(await solr_con.query(
            solr_query, session)).get('response', {}).get('docs', [])
        temp_red_query = red_query.format(indx=int(start / rows),
            skus_list=extract_solr_products(solr_docs))
        loop = asyncio.get_event_loop()
        red_data = {row['sku']: row for e in await asyncio.gather(
            loop.run_in_executor(executor, red_con.query, temp_red_query))
             for row in e}
        # we make this `if in red_data` because some skus are available in
        # solr but not in redshift (those are novel products)
        return [str({**doc, **red_data[doc['sku']]}) + '\n' for doc in
            solr_docs if doc['sku'] in red_data]


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
    r = loop.run_until_complete(run('dafiti', 30000))
    #print(r.errors)
    #print('done')
