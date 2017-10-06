# Copyright 2017 Apcera Inc. All rights reserved.

import sys
import unittest

from tests.client_test import *

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(ClientTest))
    test_suite.addTest(unittest.makeSuite(SubscriptionsTest))
    runner = unittest.TextTestRunner(stream=sys.stdout)
    result = runner.run(test_suite)
    if not result.wasSuccessful():
        sys.exit(1)
