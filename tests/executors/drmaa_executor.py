# -*- coding: utf-8 -*-
# LICENSE
#

import datetime
import logging
import time
import unittest


SKIP_DRMAA = False


class DrmaaExecutorTest(unittest.TestCase):

    def setUp(self):
        pass

    @unittest.skipIf(SKIP_DRMAA, 'DRMAA unsupported by this configuration')
    def test_drmaa_executor_functions(self):
        self.assertTrue(False)
