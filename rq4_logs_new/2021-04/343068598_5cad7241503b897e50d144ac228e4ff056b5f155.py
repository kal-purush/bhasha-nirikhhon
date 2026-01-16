import unittest

from test.utils import *


class TestStream(unittest.TestCase):
    def test_stream_io(self):
        self.assertEqual(exec_ok('''
            f = <i> (n) <o>  {
                n + << i >> o;
            };
            fc = f(18);
            1 >> fc<i>;
            << fc<o>
        '''), '19')

    def test_stream_out(self):
        self.assertEqual(exec_ok('''
            f = (s) <out> {
                s >> out;
            };
            fc = f('hello');
            << fc<out>
        '''), "'hello'")

    def test_connect_fns(self):
        self.assertEqual(exec_ok('''
            f = (n) <out> {
                n + 1 >> out;
            };
            fc = f(1);
            g = () <out> {
                << fc<out> + 1 >> out;
            };
            << g()<out>
        '''), '3')

    def test_name_clash(self):
        self.assertEqual(exec_ko('''
            f = <a> () <a> { 1; }
        '''), "line 2:16 Name clash between input and output streams")

    def test_read_too_much(self):
        self.assertEqual(exec_ko('''
            f = (s) <out> {
                s >> out;
            };
            fc = f('hello');
            << fc<out>;
            << fc<out>
        '''), "line 7:12 Deadlock detected")

    def test_read_illegal_rw(self):
        self.assertEqual(exec_ko('''
            f = <in> () <out> {
                << out;
            };
            f()
        '''), "line 3:16 Cannot read from output within function")
        self.assertEqual(exec_ko('''
            f = <in> () <out> {
                1 >> in;
            };
            f()
        '''), "line 3:16 Cannot write to input within function")
        self.assertEqual(exec_ko('''
            f = <in> () <out> {
                1 >> out;
            };
            1 >> f()<out>
        '''), "line 5:12 Cannot write to output of another function")
        self.assertEqual(exec_ko('''
            f = <in> () <out> {
                1 >> out;
            };
            << f()<in>
        '''), "line 5:12 Cannot read from input to another function")

    def test_nested(self):
        self.assertEqual(exec_ok('''
            f = <in> (n) <out> {
                g = <in> (n) <out> {
                    << in * n >> out;
                };
                gc = g(n + 1);
                << in + n >> gc<in>;
                << gc<out> >> out;
            };
            fc = f(0.5);
            2 >> fc<in>;
            << fc<out>
        '''), '3.75')
        self.assertEqual(exec_ko('''
            f = <in> (n) <out> {
                g = <in> (n) <out> {
                    1 >> out;
                    1 >> in;
                };
                g(n + 1);
            };
            f(1)
        '''), "line 5:20 Cannot write to input within function")
        self.assertEqual(exec_ko('''
            f = <in> (n) <out> {
                g = <in> (n) <out> {
                    1 >> out;
                    << out;
                };
                g(n + 1);
            };
            f(1)
        '''), "line 5:20 Cannot read from output within function")