# Copyright (c) 2018 Evalf
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import treelog, unittest, contextlib, tempfile, os, sys, hashlib, io

class Log(unittest.TestCase):

  maxDiff = None

  @contextlib.contextmanager
  def assertSilent(self):
    with contextlib.redirect_stdout(write(self.fail)):
      yield

  @treelog.withcontext
  def generate_id(self, id):
    with treelog.warningfile('test.dat', 'wb', id=id) as f:
      f.write(b'test3')

  def generate(self):
    treelog.user('my message')
    with treelog.infofile('test.dat', 'wb') as f:
      f.write(b'test1')
    with treelog.context('my context'):
      for i in treelog.iter('iter', 'abc'):
        treelog.info(i)
      with treelog.context('empty'):
        pass
      treelog.error('multiple..\n  ..lines')
      with treelog.userfile('test.dat', 'wb+') as f:
        treelog.info('generating')
        f.write(b'test2')
    self.generate_id(b'abc')
    with treelog.errorfile('same', 'wb', id=b'abc') as f:
      f.write(b'test3')

  def test_output(self):
    with self.assertSilent(), self.output_tester() as log, treelog.set(log):
      self.generate()

class StdoutLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with capture() as writes:
      yield treelog.StdoutLog()
    self.assertEqual(''.join(writes),
      'my message\n'
      'test.dat\n'
      'my context > iter 0 (17%) > a\n'
      'my context > iter 1 (50%) > b\n'
      'my context > iter 2 (83%) > c\n'
      'my context > multiple..\n'
      '  ..lines\n'
      'my context > test.dat > generating\n'
      'my context > test.dat\n'
      'generate_id > test.dat\n'
      'same\n')

  def test_iter_warn(self):
    log = treelog.StdoutLog()
    with self.assertWarns(ResourceWarning):
      for i in log.iter('range', range(9)):
        if i == 2:
          break

  def test_iter_close(self):
    log = treelog.StdoutLog()
    r = log.iter('range', range(9))
    for i in r:
      if i == 2:
        r.close()
    self.assertEqual(i, 2)

  def test_iter_context(self):
    log = treelog.StdoutLog()
    with log.iter('range', range(9)) as r:
      for i in r:
        if i == 2:
          break
    self.assertFalse(r.close())

class RichOutputLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with capture() as writes:
      yield treelog.RichOutputLog(interval=99)
    self.assertEqual(writes, [
      '\x1b[K\x1b[1;34mmy message\x1b[0m\n',
      '\x1b[Ktest.dat\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · iter 0 (17%) · \x1b[0ma\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · iter 1 (50%) · \x1b[0mb\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · iter 2 (83%) · \x1b[0mc\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · \x1b[1;31mmultiple..\n  ..lines\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · test.dat · \x1b[0mgenerating\x1b[0m\n',
      '\x1b[K\x1b[1;30mmy context · \x1b[1;34mtest.dat\x1b[0m\n',
      '\x1b[K\x1b[1;30mgenerate_id · \x1b[0;31mtest.dat\x1b[0m\n',
      '\x1b[K\x1b[1;31msame\x1b[0m\n'])

  def test_thread(self):
    import _thread
    lock = _thread.allocate_lock()
    interval = .1
    with waitable_capture(lock) as writes:
      log = treelog.RichOutputLog(interval=interval)
      lock.acquire(0)
      with log.context('A'):
        self.assertTrue(lock.acquire(timeout=interval*10), 'timed out')
      log.info('B')
      lock.acquire(0)
      with log.context('C'):
        with log.context('D'):
          with log.context('E'):
            pass
          self.assertTrue(lock.acquire(timeout=interval*10), 'timed out')
    self.assertEqual(writes, [
      '\x1b[K\x1b[1;30mA\x1b[0m\r',
      '\x1b[KB\x1b[0m\n',
      '\x1b[K\x1b[1;30mC · D\x1b[0m\r'])

class DataLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      yield treelog.DataLog(tmpdir)
      self.assertEqual(set(os.listdir(tmpdir)), {'test-1.dat', 'test-2.dat', 'test-3.dat', 'same-1', '.id'})
      self.assertEqual(os.listdir(os.path.join(tmpdir, '.id')), ['616263'])
      with open(os.path.join(tmpdir, 'test-1.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test1')
      with open(os.path.join(tmpdir, 'test-2.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test2')
      with open(os.path.join(tmpdir, 'test-3.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test3')
      with open(os.path.join(tmpdir, 'same-1'), 'rb') as f:
        self.assertEqual(f.read(), b'test3')
      with open(os.path.join(tmpdir, '.id', '616263'), 'rb') as f:
        self.assertEqual(f.read(), b'test3')

  @unittest.skipIf(not treelog._io.supports_fd, 'dir_fd not supported on platform')
  def test_move_outdir(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      outdira = os.path.join(tmpdir, 'a')
      outdirb = os.path.join(tmpdir, 'b')
      log = treelog.DataLog(outdira)
      os.rename(outdira, outdirb)
      os.mkdir(outdira)
      with log.infofile('dat', 'wb') as f:
        pass
      self.assertEqual(os.listdir(outdirb), ['dat-1'])
      self.assertEqual(os.listdir(outdira), [])

  def test_remove_on_failure(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      log = treelog.DataLog(tmpdir)
      with self.assertRaises(RuntimeError):
        with log.infofile('dat', 'wb') as f:
          f.write(b'test')
          raise RuntimeError
      self.assertFalse(os.listdir(tmpdir))

  def test_remove_on_failure_id(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      log = treelog.DataLog(tmpdir)
      with self.assertRaises(RuntimeError):
        with log.infofile('dat', 'wb', id=b'abc') as f:
          f.write(b'test')
          raise RuntimeError
      self.assertEqual(os.listdir(tmpdir), ['.id'])
      self.assertFalse(os.listdir(os.path.join(tmpdir, '.id')))

  def test_open_id(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      log = treelog.DataLog(tmpdir)
      with log.infofile('dat1', 'wb', id=b'abc') as f:
        pass
      self.assertEqual(os.listdir(os.path.join(tmpdir, '.id')), ['616263'])
      with log.infofile('dat2', 'wb', id=b'abc') as f:
        self.assertFalse(f)

class HtmlLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with self.assertSilent(), treelog.HtmlLog(tmpdir, title='test') as htmllog:
        yield htmllog
      self.assertEqual(htmllog.filename, 'log.html')
      from treelog.htm import CSS, JS
      css = hashlib.sha1(CSS.encode()).hexdigest() + '.css'
      js = hashlib.sha1(JS.encode()).hexdigest() + '.js'
      self.assertEqual(set(os.listdir(tmpdir)), {'log.html', js, css, '616263.dat', '616263',
        'b444ac06613fc8d63795be9ad0beaf55011936ac.dat', '109f4b3c50d7b0df729d299bc6f8e9ef9066971f.dat'})
      with open(os.path.join(tmpdir, 'log.html'), 'r') as f:
        lines = f.readlines()
      self.assertEqual(lines, [
        '<!DOCTYPE html>\n',
        '<html>\n',
        '<head>\n',
        '<meta charset="UTF-8"/>\n',
        '<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, minimum-scale=1, user-scalable=no"/>\n',
        '<title>test</title>\n',
        '<script src="{}"></script>\n'.format(js),
        '<link rel="stylesheet" type="text/css" href="{}"/>\n'.format(css),
        '</head>\n',
        '<body>\n',
        '<div id="log">\n',
        '<div class="item" data-loglevel="2">my message</div>\n',
        '<div class="item" data-loglevel="1"><a href="b444ac06613fc8d63795be9ad0beaf55011936ac.dat">test.dat</a></div>\n',
        '<div class="context"><div class="title">my context</div><div class="children">\n',
        '<div class="context"><div class="title">iter 0 (17%)</div><div class="children">\n',
        '<div class="item" data-loglevel="1">a</div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="context"><div class="title">iter 1 (50%)</div><div class="children">\n',
        '<div class="item" data-loglevel="1">b</div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="context"><div class="title">iter 2 (83%)</div><div class="children">\n',
        '<div class="item" data-loglevel="1">c</div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="item" data-loglevel="4">multiple..\n',
        '  ..lines</div>\n',
        '<div class="context"><div class="title">test.dat</div><div class="children">\n',
        '<div class="item" data-loglevel="1">generating</div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="item" data-loglevel="2"><a href="109f4b3c50d7b0df729d299bc6f8e9ef9066971f.dat">test.dat</a></div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="context"><div class="title">generate_id</div><div class="children">\n',
        '<div class="item" data-loglevel="3"><a href="616263.dat">test.dat</a></div>\n',
        '</div><div class="end"></div></div>\n',
        '<div class="item" data-loglevel="4"><a href="616263">same</a></div>\n',
        '</div></body></html>\n'])
      with open(os.path.join(tmpdir, 'b444ac06613fc8d63795be9ad0beaf55011936ac.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test1')
      with open(os.path.join(tmpdir, '109f4b3c50d7b0df729d299bc6f8e9ef9066971f.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test2')
      with open(os.path.join(tmpdir, '616263.dat'), 'rb') as f:
        self.assertEqual(f.read(), b'test3')
      with open(os.path.join(tmpdir, '616263'), 'rb') as f:
        self.assertEqual(f.read(), b'test3')

  @unittest.skipIf(not treelog._io.supports_fd, 'dir_fd not supported on platform')
  def test_move_outdir(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      outdira = os.path.join(tmpdir, 'a')
      outdirb = os.path.join(tmpdir, 'b')
      with silent, treelog.HtmlLog(outdira) as log:
        os.rename(outdira, outdirb)
        os.mkdir(outdira)
        with log.infofile('dat', 'wb') as f:
          pass
      self.assertIn('da39a3ee5e6b4b0d3255bfef95601890afd80709', os.listdir(outdirb))

  def test_remove_on_failure(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with silent, treelog.HtmlLog(tmpdir) as log, self.assertRaises(RuntimeError):
        with log.infofile('dat', 'wb') as f:
          f.write(b'test')
          raise RuntimeError
      self.assertEqual(len(os.listdir(tmpdir)), 3)

  def test_remove_on_failure_id(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with silent, treelog.HtmlLog(tmpdir) as log, self.assertRaises(RuntimeError):
        with log.infofile('dat', 'wb', id=b'abc') as f:
          f.write(b'test')
          raise RuntimeError
      self.assertEqual(len(os.listdir(tmpdir)), 3)

class RecordLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    recordlog = treelog.RecordLog()
    yield recordlog
    self.assertEqual(recordlog._messages, [
      ('write', 'my message', 2),
      ('open_enter', 'test.dat', 'wb', 1, None),
      ('open_exit', b'test1', None, None, None),
      ('context_enter', 'my context'),
      ('context_enter', 'iter 0 (17%)'),
      ('write', 'a', 1),
      ('context_exit', None, None, None),
      ('context_enter', 'iter 1 (50%)'),
      ('write', 'b', 1),
      ('context_exit', None, None, None),
      ('context_enter', 'iter 2 (83%)'),
      ('write', 'c', 1),
      ('context_exit', None, None, None),
      ('write', 'multiple..\n  ..lines', 4),
      ('open_enter', 'test.dat', 'wb+', 2, None),
      ('write', 'generating', 1),
      ('open_exit', b'test2', None, None, None),
      ('context_exit', None, None, None),
      ('context_enter', 'generate_id'),
      ('open_enter', 'test.dat', 'wb', 3, b'abc'),
      ('open_exit', b'test3', None, None, None),
      ('context_exit', None, None, None),
      ('open_enter', 'same', 'wb', 4, b'abc'),
      ('open_exit', b'test3', None, None, None)])
    for Log in StdoutLog, RichOutputLog, DataLog, HtmlLog:
      with self.subTest('replay to {}'.format(Log.__name__)), Log.output_tester(self) as log:
        recordlog.replay(log)

  def test_replay_in_current(self):
    recordlog = treelog.RecordLog()
    recordlog.info('test')
    with self.assertSilent(), treelog.set(treelog.LoggingLog()), self.assertLogs('nutils'):
      recordlog.replay()

class TeeLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with DataLog.output_tester(self) as datalog, \
         RecordLog.output_tester(self) as recordlog, \
         RichOutputLog.output_tester(self) as richoutputlog:
      yield treelog.TeeLog(richoutputlog, treelog.TeeLog(datalog, recordlog))

  def test_open_devnull_devnull(self):
    teelog = treelog.TeeLog(treelog.StdoutLog(), treelog.StdoutLog())
    with silent, teelog.infofile('test', 'wb') as f:
      self.assertFalse(f)

  def test_open_devnull_file(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      teelog = treelog.TeeLog(treelog.StdoutLog(), treelog.DataLog(tmpdir))
      with silent, teelog.infofile('test', 'wb') as f:
        self.assertIsInstance(f, io.BufferedWriter)
        f.write(b'test')
      with open(os.path.join(tmpdir, 'test-1'), 'rb') as f:
        self.assertEqual(f.read(), b'test')

  def test_open_file_devnull(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      teelog = treelog.TeeLog(treelog.DataLog(tmpdir), treelog.StdoutLog())
      with silent, teelog.infofile('test', 'wb') as f:
        self.assertIsInstance(f, io.BufferedWriter)
        f.write(b'test')
      with open(os.path.join(tmpdir, 'test-1'), 'rb') as f:
        self.assertEqual(f.read(), b'test')

  def test_open_file_file(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      teelog = treelog.TeeLog(treelog.DataLog(tmpdir), treelog.DataLog(tmpdir))
      with teelog.infofile('test', 'wb') as f:
        self.assertIsInstance(f, io.BufferedRandom)
        f.write(b'test')
      with open(os.path.join(tmpdir, 'test-1'), 'rb') as f:
        self.assertEqual(f.read(), b'test')
      with open(os.path.join(tmpdir, 'test-2'), 'rb') as f:
        self.assertEqual(f.read(), b'test')

  def test_open_seekable_file(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      recordlog = treelog.RecordLog()
      teelog = treelog.TeeLog(recordlog, treelog.DataLog(tmpdir))
      with teelog.infofile('test', 'wb', id=b'abc') as f:
        self.assertIsInstance(f, io.BufferedRandom)
        f.write(b'test')
      self.assertEqual(recordlog._seen[b'abc'], b'test')
      with open(os.path.join(tmpdir, 'test-1'), 'rb') as f:
        self.assertEqual(f.read(), b'test')

class FilterLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    recordlog = treelog.RecordLog()
    yield treelog.FilterLog(recordlog, minlevel=2)
    self.assertEqual(recordlog._messages, [
      ('write', 'my message', 2),
      ('context_enter', 'my context'),
      ('write', 'multiple..\n  ..lines', 4),
      ('open_enter', 'test.dat', 'wb+', 2, None),
      ('open_exit', b'test2', None, None, None),
      ('context_exit', None, None, None),
      ('context_enter', 'generate_id'),
      ('open_enter', 'test.dat', 'wb', 3, b'abc'),
      ('open_exit', b'test3', None, None, None),
      ('context_exit', None, None, None),
      ('open_enter', 'same', 'wb', 4, b'abc'),
      ('open_exit', b'test3', None, None, None)])

class LoggingLog(Log):

  @contextlib.contextmanager
  def output_tester(self):
    with self.assertLogs('nutils') as cm:
      yield treelog.LoggingLog()
    self.assertEqual(cm.output, [
      'Level 25:nutils:my message',
      'INFO:nutils:test.dat',
      'INFO:nutils:my context > iter 0 (17%) > a',
      'INFO:nutils:my context > iter 1 (50%) > b',
      'INFO:nutils:my context > iter 2 (83%) > c',
      'ERROR:nutils:my context > multiple..\n  ..lines',
      'INFO:nutils:my context > test.dat > generating',
      'Level 25:nutils:my context > test.dat',
      'WARNING:nutils:generate_id > test.dat',
      'ERROR:nutils:same'])

del Log # hide from unittest discovery

## INTERNALS

class write:
  def __init__(self, write):
    self.write = write

@contextlib.contextmanager
def capture():
  writes = []
  with contextlib.redirect_stdout(write(writes.append)):
    yield writes

@contextlib.contextmanager
def waitable_capture(lock):
  writes = []
  @write
  def release_and_append(text):
    if lock.locked():
      lock.release()
    writes.append(text)
  with contextlib.redirect_stdout(release_and_append):
    yield writes

@write
def devnull(text):
  pass

silent = contextlib.redirect_stdout(devnull)

# vim:sw=2:sts=2:et