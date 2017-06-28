# -*- coding: utf-8 -*-
# LICENSE
#

import sys
import datetime
import unittest
import subprocess

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
        self.dagbag = DagBag(include_examples=True)
        self.test_commands = {
            ('success', 'command', datetime.datetime.now()): 'echo 1',
            ('failure', 'command', datetime.datetime.now()): 'exit 1',
        }

    def test_drmaa_executor_run(self):
        executor = DrmaaExecutor()
        mock_drmaa.Session()._reset()
        executor.start()

        for key, command in self.test_commands.items():
            executor.execute_async(key, command)

        self.assertEqual(executor.session.runJob.call_count,
                         len(self.test_commands))

    def test_drmaa_executor_queue(self):
        executor = DrmaaExecutor()
        mock_drmaa.Session()._reset()
        executor.start()

        for key, command in self.test_commands.items():
            task_instance = mock.Mock()
            task_instance.key = key
            executor.queue_command(task_instance, command)

        executor.heartbeat()

        self.assertEqual(executor.session.jobStatus.call_count,
                         len(self.test_commands))
        self.assertEqual(executor.session.wait.call_count,
                         len(self.test_commands))

        executor.end()

        self.assertTrue(executor.session.synchronize.called)

        states = executor.get_event_buffer()
        for key, state in states.items():
            if key[0] == 'success':
                self.assertEqual(state, State.SUCCESS)
            elif key[0] == 'failure':
                self.assertEqual(state, State.FAILED)

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
