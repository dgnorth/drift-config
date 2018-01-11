# -*- coding: utf-8 -*-
import unittest

from driftconfig.testhelpers import create_test_domain


class TestTestHelpers(unittest.TestCase):

    def test_create_test_domain(self):
        create_test_domain()


if __name__ == '__main__':
    unittest.main()
