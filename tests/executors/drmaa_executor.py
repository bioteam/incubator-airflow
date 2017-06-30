# -*- coding: utf-8 -*-
# LICENSE
#

import sys
import datetime
import unittest
import contextlib
import subprocess

from airflow import configuration
from airflow.models import DagBag, State
from airflow.jobs import BackfillJob

try:
    from unittest import mock
except ImportError:
    import mock

mock_drmaa = mock.Mock()


class MockDrmaaSession(mock.Mock):
    _exit_code = []

    def _run_job(jt):
        try:
            subprocess.check_call([jt.remoteCommand] + jt.args)
            MockDrmaaSession._exit_code.append(0)
        except subprocess.CalledProcessError as e:
            MockDrmaaSession._exit_code.append(e.returncode)
        return len(MockDrmaaSession._exit_code) - 1
    runJob = mock.Mock(side_effect=_run_job)

    jobStatus = mock.Mock(return_value=mock_drmaa.JobState.DONE)

    def _wait(i, t):
        m = mock.Mock()
        m.exitStatus = MockDrmaaSession._exit_code[i]
        return m
    wait = mock.Mock(side_effect=_wait)

    def _reset(self):
        self.runJob.reset_mock()
        self.jobStatus.reset_mock()
        self.wait.reset_mock()
        self.reset_mock()

mock_drmaa.Session.side_effect = MockDrmaaSession

sys.modules['drmaa'] = mock_drmaa
from airflow.executors.drmaa_executor import DrmaaExecutor  # noqa

DEFAULT_DATE = datetime.datetime(2017, 1, 1)


class DrmaaExecutorTest(unittest.TestCase):

    def setUp(self):
        self.test_commands = {
            ('success', 'command', datetime.datetime.now()): 'echo 1',
            ('failure', 'command', datetime.datetime.now()): 'exit 1',
        }
        self.executor = DrmaaExecutor()
        mock_drmaa.Session()._reset()
        self.executor.start()

    def test_drmaa_executor_run(self):
        for key, command in self.test_commands.items():
            self.executor.execute_async(key, command)

        self.assertEqual(self.executor.session.runJob.call_count,
                         len(self.test_commands))

    def test_drmaa_executor_queue_command(self):
        for key, command in self.test_commands.items():
            task_instance = mock.Mock()
            task_instance.key = key
            self.executor.queue_command(task_instance, command)

        self.executor.heartbeat()

        self.assertEqual(self.executor.session.jobStatus.call_count,
                         len(self.test_commands))
        self.assertEqual(self.executor.session.wait.call_count,
                         len(self.test_commands))

        self.executor.end()

        self.assertTrue(self.executor.session.synchronize.called)

        states = self.executor.get_event_buffer()
        for key, state in states.items():
            if key[0] == 'success':
                self.assertEqual(state, State.SUCCESS)
            elif key[0] == 'failure':
                self.assertEqual(state, State.FAILED)


class DrmaaExecutorConfigurationTest(unittest.TestCase):

    def setUp(self):
        self.test_command_key = ('test', 'command', datetime.datetime.now())
        self.test_command = 'echo conftest'

    @contextlib.contextmanager
    def configuration_context(self, confd):
        cache = {}
        for key, value in confd.items():
            if configuration.has_option(*key):
                cache[key] = configuration.get(*key)
            else:
                cache[key] = None
            configuration.set(key[0], key[1], value)

        yield

        for key, value in confd.items():
            if cache[key] is None:
                configuration.remove_option(*key)
            else:
                configuration.set(key[0], key[1], cache[key])

    def run_test(self, queue=None):
        executor = DrmaaExecutor()
        mock_drmaa.Session()._reset()
        executor.start()
        
        executor.execute_async(self.test_command_key, self.test_command,
                               queue=queue)
        return executor
                
    def test_drmaa_executor_config_options(self):
        with self.configuration_context({
                ('drmaa', 'wrapper_script'): '/bin/true'}):

            executor = self.run_test()
            
            self.assertEqual(
                executor.session.runJob.call_args[0][0].remoteCommand,
                '/bin/true')

    def test_drmaa_executor_native_specification(self):
        with self.configuration_context({
                ('drmaa', 'native_spec'): '-b y'}):
            executor = self.run_test()
            self.assertIn(
                '-b y',
                executor.session.runJob.call_args[0][0].nativeSpecification
            )

    def test_drmaa_executor_native_specification_sge(self):
        with self.configuration_context({
                ('drmaa', 'scheduler_type'): 'sge',
                ('drmaa', 'sge_parallel_env'): 'smp'}):
            executor = self.run_test('all.q:4:16G:h_rt=12:00:00')
            spec = executor.session.runJob.call_args[0][0].nativeSpecification
            self.assertIn('-q all.q', spec)
            self.assertIn('-pe smp 4', spec)
            self.assertIn('-l mem_free=16G', spec)
            self.assertIn('-l h_rt=12:00:00', spec)
    
    def test_drmaa_executor_native_specification_pbs(self):
        with self.configuration_context({
                ('drmaa', 'scheduler_type'): 'pbs'}):
            executor = self.run_test('defq:2:16GB:walltime=12:00:00')
            spec = executor.session.runJob.call_args[0][0].nativeSpecification
            self.assertIn('-q defq', spec)
            self.assertIn('-l ppn=2', spec)
            self.assertIn('-l mem=16G', spec)
            self.assertIn('-l walltime=12:00:00', spec)

    def test_drmaa_executor_native_specification_slurm(self):
        with self.configuration_context({
                ('drmaa', 'scheduler_type'): 'slurm'}):
            executor = self.run_test('batch:8:1024:gpu:kepler:2')
            spec = executor.session.runJob.call_args[0][0].nativeSpecification
            self.assertIn('-p batch', spec)
            self.assertIn('-N 1 -n 8', spec)
            self.assertIn('--mem-per-cpu=1024', spec)
            self.assertIn('--gres=gpu:kepler:2', spec)

    def test_drmaa_executor_native_specification_lsf(self):
        with self.configuration_context({
                ('drmaa', 'scheduler_type'): 'lsf'}):
            executor = self.run_test('pq:4:16:foo==bar')
            spec = executor.session.runJob.call_args[0][0].nativeSpecification
            self.assertIn('-q pq', spec)
            self.assertIn('-n 4', spec)
            self.assertIn('-M 16', spec)
            self.assertIn('-R foo==bar', spec)
        
                
class DrmaaExecutorIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(include_examples=True)
                
    def test_backfill_integration(self):
        dags = [
            dag for dag in self.dagbag.dags.values()
            if dag.dag_id in ['example_bash_operator']
            ]

        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)

            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_first_depends_on_past=True,
                executor=DrmaaExecutor())
            job.run()
