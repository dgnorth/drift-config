# -*- coding: utf-8 -*-
import sys
import unittest
import os.path
import importlib

"""
Test the import of various top level modules to make sure they compile
"""


class ImportTestCase(unittest.TestCase):
    def test_driftconfig(self):
        importlib.import_module("driftconfig")


class ScriptImportTestCase(unittest.TestCase):
    script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

    def setUp(self):
        sys.path.append(self.script_dir)

    def tearDown(self):
        sys.path.pop()

    def test_testconfig(self):
        importlib.import_module("testconfig")

    def test_update_trigger(self):
        # must use this because the module contains a dash
        __import__('update-trigger', globals(), locals(), [], 0)
