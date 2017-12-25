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


"""Worker module used to run background operations"""


import utils
from flask import Flask, request 
from config import config
from connector.gcp import GCPService
from datetime import datetime, timedelta


app = Flask(__name__)
gcp_service = GCPService() 

@app.route("/update_dashboard_tables", methods=['POST'])
def update_search_tables():
    """Creates a new table and deletes previous table if condition mets."""
    setup = config['jobs']['update_dashboard_tables'] 
    date = (utils.yesterday_date().strftime("%Y%m%d") if
            'date' not in request.form else
            utils.process_url_date(request.form.get('date')))

    query_job_body = utils.search_query_job_body(**dict(setup.items() +
        [('date', date)]))

    job = gcp_service.bigquery.execute_job(setup['project_id'],
        query_job_body)
    gcp_service.bigquery.poll_job(job)

    gcp_service.bigquery.delete_table(project_id=setup['dest_project_id'],
        dataset_id=setup['dest_dataset_id'],
        table_id=setup['dest_table_id'].format((datetime.now() - 
            timedelta(days=1 + setup['total_days'])).strftime("%Y%m%d")))

    return "finished"
