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


import os

import nox


def session_unit_gae(session):
    """This session tests only the tests associated to google AppEngine
    folder. To run it, type `nox --session unit_gae`
    """
    session.interpreter = 'python2.7'
    session.virtualenv_dirname = 'unit-gae'

    session.install('-r', 'tests/unit/data/gae/test_requirements.txt')
    session.install('pytest', 'pytest-cov', 'mock')

    if not os.path.isdir('/google-cloud-sdk/platform/google_appengine/'):
        raise RuntimeError("Please install gcloud components for app engine"
                           " in order to simulate an AppEngine environment "
                           " for testing")

    # we set ``gae/exporter`` in PYTHONPATH as well since this becomes
    # the root directory when App Engine starts the wsgi server
    session.env = {'PYTHONPATH': (':/google-cloud-sdk/platform/' 
        'google_appengine/:./:./gae/:/google-cloud-sdk/platform/'
        'google_appenigne/lib/yaml/lib')}

    session.run(
        'py.test',
        'tests/unit/gae/',
        '--cov=.',
        '--cov-report=html')


def session_unit_feeds(session):
    """This session tests only the tests associated to the feeds jobs 
    folder. To run it, type `nox -s unit_feeds`
    """
    session.interpreter = 'python3.6'
    session.virtualenv_dirname = 'unit-feeds'

    #session.install('-r', 'tests/unit/data/gae/test_requirements.txt')
    session.install('pytest', 'pytest-cov', 'mock')

    session.env = {'PYTHONPATH': './'}

    session.run(
        'py.test',
        'tests/unit/feeds/connectors/test_solr.py',
        '--cov=.',
        '--cov-report=html')
