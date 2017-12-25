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


"""Scheduler to run tasks in background in GAE."""


import datetime

from google.appengine.api import taskqueue
import utils


class SchedulerJob(object):
    """Job queue tasks.

    :type url: str
    :param url: url to trigger for task call.

    :type target: str
    param target: name of service to trigger when running background tasks.
    """
    
    def __init__(self, url, target):
        self.task = None
        self.url = url 
        self.target = target

    def run(self, args): 
        """Executes the specified job in `self.url` and `self.target`.

        :type args: dict
        :param args: dictionary with arguments to setup the job, such as which
                     `date` to process data and so on.

        :raises ValueError: on `self.url` and `self.target` being False.
        """
        if not (self.url and self.target):
            raise ValueError("Please specify `URL` and `target`")
        task = taskqueue.add(url=self.url, target=self.target,
            params=args)
        self.task = task

    def __str__(self):
        if not self.task:
            return 'No task has been enqueued so far'
        return "Task {} enqued, ETA {}".format(self.task.name, self.task.eta) 
