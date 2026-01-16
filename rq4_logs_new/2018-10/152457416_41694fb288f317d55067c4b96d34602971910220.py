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

version = '1.0b0'

import sys, functools, contextlib

from .abc import Log
from .fwd import TeeLog, FilterLog
from .ctx import StdoutLog, RichOutputLog, LoggingLog
from .dat import DataLog
from .rec import RecordLog
from .htm import HtmlLog

current = FilterLog(TeeLog(StdoutLog(), DataLog()), minlevel=1)

@contextlib.contextmanager
def set(logger):
  '''Set logger as current.'''

  global current
  old = current
  try:
    current = logger
    yield logger
  finally:
    current = old

def add(logger):
  '''Add logger to current.'''

  return set(TeeLog(current, logger))

def withcontext(f):
  '''Decorator; executes the wrapped function in its own logging context.'''

  @functools.wraps(f)
  def wrapped(*args, **kwargs):
    with current.context(f.__name__):
      return f(*args, **kwargs)
  return wrapped

def __getattr__(name):
  return getattr(current, name)

if sys.version_info < (3,7):
  def _factory(name):
    def wrapper(*args, **kwargs):
      return __getattr__(name)(*args, **kwargs)
    wrapper.__doc__ = getattr(abc.Log, name).__doc__
    wrapper.__name__ = name
    wrapper.__qualname__ = name
    return wrapper
  locals().update((name, _factory(name)) for name in dir(abc.Log))
  del _factory

# vim:sw=2:sts=2:et