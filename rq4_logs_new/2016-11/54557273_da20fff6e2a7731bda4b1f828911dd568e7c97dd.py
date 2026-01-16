#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Ivo Romario <ivo.romario@gmail.com>
# License:

import unittest
from pydst.lists.queue import Queue


class TestQueue(unittest.TestCase):

    def setUp(self):
        # creating an empty queue
        self.q = Queue()

    def test_is_empty(self):
        """Checking if queue is empty"""
        self.assertTrue(self.q.is_empty(), 'Queue should be empty')

    def test_enqueue(self):
        """Adding item to queue"""
        self.q.enqueue(1)
        self.assertEqual(1, len(self.q), 'Size doesn\'t match')


    def test_dequeue(self):
        """Removing item from queue"""
        self.q.enqueue(1)
        self.q.enqueue(2)
        size = self.q.size
        self.q.dequeue()
        self.assertEqual(size-1, len(self.q), 'Size doesn\'t match')
