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


"""General functions to be used throughout the services modules"""


import datetime
import uuid
import time


def yesterday_date():
    """Returns datetime for yesterday value

    :rtype: `datetime.datetime`
    :returns: yesterday's datetime
    """
    return (datetime.datetime.now() +
             datetime.timedelta(days=-1))


def search_query_job_body(**kwargs):
    """Returns the body to be used in a query job.

    :type kwargs:
      :type date: str
      :param date: date to format query period.

      :type source.query_path: str
      :param query: query string to run against BQ.

      :type source.project_id: str
      :param source.project: project where to run query from.

      :type source.dataset_id: str
      :param source.dataset_id: dataset where to run query from.

      :type source.table_id: str
      :param source.table_id: table where to run query from.

      :type destination.table_id: str
      :param destination.table_id: table_id where results should be saved.

      :type destination.dataset_id: str
      :param destination.dataset_id: dataset_id where results should be saved.

      :type destination.project_id: str
      :param destination.project_id: project_id where results should be saved.

    :rtype: dict
    :returns: dict containing body to setup job execution.
    """
    query = load_file_content(**kwargs)
    return {'jobReference': {
                'projectId': kwargs['project_id'],
                'jobId': str(uuid.uuid4())
                },
            'configuration': {
                'query': {
                    'destinationTable': {
                        'datasetId': kwargs['dest_dataset_id'],
                        'tableId': kwargs['dest_table_id'].format(
                            kwargs['date']),
                        'projectId': kwargs['dest_project_id']
                         },
                    'maximumBytesBilled': 100000000000, #100 GBs max is allowed
                    'query': query,
                    'useLegacySql': False
                    }
                }
            }


def load_file_content(**kwargs):
    """Reads the string from a source file and formats it using ``kwargs``.

    :type kwargs: dict
    :param kwargs: it contains keys and values to render query template.

    :rtype: str
    :returns: file string after format processing with ``kwargs`` input.
    """
    return open(kwargs.get('query_path')).read().format(**kwargs).strip()


def format_date(input_date, format="%Y-%m-%d"):
    """Changes input date to a new format.

    :type input_date: str
    :param input_date: date string of format "%Y%m%d".

    :type format: str
    :param format: new format to port input date.

    :rtype: str
    :returns: date string in format `format`.
    """
    return datetime.datetime.strptime(input_date, "%Y%m%d").strftime(
        format)


def process_url_date(date):
    """Gets the variable ``date`` from URL.

    :type date: str 
    :param date: date to process. 

    :raises: `ValueError` if ``date`` is not in format "%Y%m%d" and is
             not null.

    :rtype: str
    :returns: `None` is `date` is empty or a string representation of date
    """
    # if ``date`` is defined then it was sent as parameter in the URL request
    if date:
        try:
            datetime.datetime.strptime(date, "%Y%m%d")
        except ValueError:
            raise
    return date
