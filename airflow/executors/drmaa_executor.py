# -*- coding: utf-8 -*-
# LICENSE
#

import os
import re

import drmaa

from airflow import configuration
from airflow.utils.file import mkdirs
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import State

from past.builtins import basestring


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
        if queue is None:
            if configuration.has_option('drmaa', 'default_queue'):
                queue = configuration.get('drmaa', 'default_queue')
            else:
                queue = ""
        self.logger.info("[drmaa] queueing {key} through DRMAA, "
                         "queue={queue}".format(**locals()))

        jt = self.session.createJobTemplate()
        jt.jobName = re.sub(r'\W+', '', '_'.join(key[:2]))
        jt.workingDirectory = configuration.get('core', 'airflow_home')
        jt.nativeSpecification = self.make_native_spec(queue)
        jt.joinFiles = False
        log_base = os.path.join(
            os.path.expanduser(configuration.get('core', 'base_log_folder')),
            'drmaa_executor',
            key[0]
        )
        if not os.path.exists(log_base):
            mkdirs(log_base, 0o775)
        jt.outputPath = os.path.join(log_base, "{dag}.{date}.out".format(
            dag=key[1], date=key[2].isoformat()))
        jt.errorPath = os.path.join(log_base, "{dag}.{date}.err".format(
            dag=key[1], date=key[2].isoformat()))

        job_args = []
        if configuration.has_option('drmaa', 'wrapper_script'):
            jt.remoteCommand = configuration.get('drmaa', 'wrapper_script')
        if not jt.remoteCommand:
            jt.remoteCommand = '/bin/sh'
            job_args = ['-c']
        if isinstance(command, basestring):
            job_args.append(command)
        else:
            job_args.append(" ".join(command))
        jt.args = job_args
        self.logger.info("[drmaa] {key} command: {cmd} {job_args}".format(
            key=key, cmd=jt.remoteCommand, job_args=job_args))

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

    def make_native_spec(self, queue):
        ns = ""
        if queue and configuration.has_option('drmaa', 'scheduler_type'):
            sched_type = configuration.get('drmaa', 'scheduler_type').lower()
            queue, cpu, mem, resources = (queue.split(':', 3) + [None] * 3)[:4]
            if sched_type == 'sge':
                ns = self.make_native_spec_sge(queue, cpu, mem, resources)
            elif sched_type == 'pbs':
                ns = self.make_native_spec_pbs(queue, cpu, mem, resources)
            elif sched_type == 'slurm':
                ns = self.make_native_spec_slurm(queue, cpu, mem, resources)
            elif sched_type == 'lsf':
                ns = self.make_native_spec_lsf(queue, cpu, mem, resources)
            else:
                warnings.warn('Scheduler type {} is not supported, ignoring '
                              'queue specification'.format(sched_type))
            
        if configuration.has_option('drmaa', 'native_spec'):
            ns += " " + configuration.get('drmaa', 'native_spec')

        return ns

    def make_native_spec_sge(self, queue, cpu, mem, resources):
        ns = []
        if queue:
            ns.append('-q ' + queue)
        if cpu:
            if configuration.has_option('drmaa', 'sge_parallel_env'):
                ns.append('-pe {} {}'.format(
                    configuration.get('drmaa', 'sge_parallel_env'),
                    cpu))
            else:
                warnings.warn('DRMAA setting sge_parallel_env is not set, '
                              'ignoring queue CPU specification')
        if mem:
            ns.append('-l mem_free=' + mem)
        if resources:
            ns.append('-l ' + resources)
        return " ".join(ns)

    def make_native_spec_pbs(self, queue, cpu, mem, resources):
        ns = []
        if queue:
            ns.append('-q ' + queue)
        if cpu:
            ns.append('-l ppn=' + cpu)
        if mem:
            ns.append('-l mem=' + mem)
        if resources:
            ns.append('-l ' + resources)
        return " ".join(ns)

    def make_native_spec_slurm(self, queue, cpu, mem, resources):
        ns = []
        if queue:
            ns.append('-p ' + queue)
        if cpu:
            ns.append('-N 1 -n ' + cpu)
        if mem:
            ns.append('--mem-per-cpu=' + mem)
        if resources:
            ns.append('--gres=' + resources)
        return " ".join(ns)

    def make_native_spec_lsf(self, queue, cpu, mem, resources):
        ns = []
        if queue:
            ns.append('-q ' + queue)
        if cpu:
            ns.append('-n ' + cpu)
        if mem:
            ns.append('-M ' + mem)
        if resources:
            ns.append('-R ' + resources)
        return " ".join(ns)
                      
