# -*- coding: utf-8 -*-
from __future__ import absolute_import
import unittest
from .estestcase import ESTestCase
from pyes.odm import Model, register_model_mappings
from pyes.mappings import *
from datetime import datetime


class TestModel(Model):
    string_val = StringField()
    int_val = IntegerField()
    date_val = DateField()

    class Meta:
        index = 'test-index'
        type = 'test-model'


class ObjectDocumentMapperTestCase(ESTestCase):
    def setUp(self):
        super(ObjectDocumentMapperTestCase, self).setUp()
        self.init_default_index()
        TestModel.default_connection = self.conn

    def testRegisterModelMappings(self):
        register_model_mappings(self.conn)
        mapping = self.conn.indices.get_mapping('test-model', 'test-index')
        props = mapping['test-model']['properties']
        self.assertEqual(props.string_val.type, 'string')
        self.assertEqual(props.int_val.type, 'integer')
        self.assertEqual(props.date_val.type, 'date')

    def testModelSave(self):
        date_val = datetime(2013, 3, 31, 14, 36, 0)
        obj = TestModel(string_val='hello', int_val=33, date_val=date_val)
        obj.save()

        retrieved = TestModel.get_by_id(obj._meta.id)
        self.assertEqual(retrieved.string_val, 'hello')
        self.assertEqual(retrieved.int_val, 33)
        self.assertEqual(retrieved.date_val, date_val)


if __name__ == "__main__":
    unittest.main()
