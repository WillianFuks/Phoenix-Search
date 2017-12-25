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


"""Factorizes scheduler to run in background."""


from scheduler import SchedulerJob


class JobsFactory(object):
    """Builds the specified job for GAE."""
    def factor_job(self, job_name):
        """Selects one of the available jobs.

        :type job_name: str
        :param job_name: name of job to build.

        :rtype: `SchedulerJob`
        :returns: scheduler that receives a `URL` and a `target` parameter
                  to run tasks in background.
        """
        job_setup = self.jobs_setup.get(job_name)
        if not job_setup: 
            raise TypeError("Please choose a valid job name.")
        return SchedulerJob(url=job_setup['url'], target=job_setup['target'])

    @property
    def jobs_setup(self):
        """Jobs and their setup to run in GAE.

        :rtype: dict
        :returns: dict where keys are jobs names and values are their
                  description.    
        """
        return {'update_dashboard_tables': {'url': '/update_dashboard_tables',
                   'target': 'worker'}}
