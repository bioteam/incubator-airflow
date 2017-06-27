# -*- coding: utf-8 -*-
# LICENSE
#

import os
import warnings

import drmaa

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import State


DRMAA_TO_STATE_MAP = {
    drmaa.JobState.UNDETERMINED: None,
    drmaa.JobState.QUEUED_ACTIVE: State.QUEUED,

    # Jobs in a hold state can usually be released from hold and will
    # return to QUEUED_ACTIVE.
    drmaa.JobState.SYSTEM_ON_HOLD: State.QUEUED,
    drmaa.JobState.USER_ON_HOLD: State.QUEUED,
    drmaa.JobState.USER_SYSTEM_ON_HOLD: State.QUEUED,

    drmaa.JobState.RUNNING: State.RUNNING,

    # Jobs in a suspended state were previously running and can resume
    # running once the suspension is lifted.
    drmaa.JobState.SYSTEM_SUSPENDED: State.RUNNING,
    drmaa.JobState.USER_SUSPENDED: State.RUNNING,

    drmaa.JobState.DONE: State.SUCCESS,
    drmaa.JobState.FAILED: State.FAILED
    }


class DrmaaExecutor(BaseExecutor):
    """
    DrmaaExecutor submits tasks via the Distributed Resource
    Management Application API to a cluster or grid computing system.

    See http://www.drmaa.org/implementations.php for a list of
    implementations of the DRMAA v1 specification.
    """
    def __init__(self):
        super(DrmaaExecutor, self).__init__(parallelism=0)

    def start(self):
        self.session = drmaa.Session()
        self.session.initialize()
        self.jobs = {}
        self.job_state = {}

    def execute_async(self, key, command, queue=None):
        self.logger.info("[drmaa] queueing {key} through DRMAA, "
                         "queue={queue}".format(**locals()))
        
        jt = self.session.createJobTemplate()
        jt.jobName = key[0]
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = '/bin/sh'
        job_args = ['-c']
        if isinstance(command, basestring):
            job_args.append(command)
        else:
            job_args.append(" ".join(command))
        jt.args = job_args
        self.logger.info("[drmaa] {key} arguments: {job_args}".format(
            key=key, job_args=job_args))

        self.jobs[key] = self.session.runJob(jt)
        self.job_state[key] = State.QUEUED
        self.logger.info("[drmaa] {key} received job ID {jid}".format(
            key=key, jid=self.jobs[key]))

        self.session.deleteJobTemplate(jt)

    def sync(self):
        for key in list(self.running.keys()):
            jobid = self.jobs[key]
            status = self.session.jobStatus(jobid)
            if self.job_state[key] != DRMAA_TO_STATE_MAP[status]:
                self.logger.info("[drmaa] {key} (id: {jid}) is {state}".format(
                    key=key, jid=jobid, state=status))
                self.job_state[key] = DRMAA_TO_STATE_MAP[status]
            
            if self.job_state[key] in State.finished():
                # collect exit status
                final = self.session.wait(
                    jobid,
                    drmaa.Session.TIMEOUT_WAIT_FOREVER)
                if final.exitStatus != 0:
                    self.logger.info("[drmaa] {key} FAILED: {final}".format(
                        key=key, final=repr(final)))
                    self.job_state[key] = State.FAILED
                else:
                    self.logger.info("[drmaa] {key} complete: {final}".format(
                        key=key, final=repr(final.resourceUsage)))
                self.change_state(key, self.job_state[key])
                del(self.job_state[key])
                del(self.jobs[key])

    def end(self):
        # wait for all jobs to finish
        self.session.synchronize(
            [self.jobs[key] for key in self.running],
            drmaa.Session.TIMEOUT_WAIT_FOREVER,
            False)
        
        self.sync()

    def terminate(self):
        for key in self.running:
            jobid = self.jobs[key]
            self.session.control(jobid, drmaa.JobControlAction.TERMINATE)

        self.session.exit()
        
