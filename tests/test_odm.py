# -*- coding: utf-8 -*-
from __future__ import absolute_import
import unittest
from .estestcase import ESTestCase
from pyes.odm import Model, register_model_mappings

class ObjectDocumentMapperTestCase(ESTestCase):
    def setUp(self):
        super(ObjectDocumentMapperTestCase, self).setUp()
        self.init_default_index()

    def testRegisteredModelMappings(self):
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()
