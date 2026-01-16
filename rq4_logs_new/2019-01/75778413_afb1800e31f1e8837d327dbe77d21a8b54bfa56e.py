import sys
import os
import pytest
import inspect
from .fixtures.dummy_functions import dummy_functions
from .fixtures.dummy_classes import FakeQueue
from hypothesis import given
from hypothesis import strategies as st
from unittest.mock import patch
import tempfile

sys.path.append('..')
import autotest_enqueuer as ate

class TestFormatJobID:
    """ tests ate.format_job_id """

    @classmethod
    def setup_class(cls):
        cls.unique_strings = {}

    @given(markus_address=st.text(), run_id=st.integers())
    def test_returns_unique_strings(self, **kw):
        """ test that unique args return unique strings """
        job_id = ate.format_job_id(**kw)
        kwargs = tuple(sorted(kw.items()))
        try:
            if kwargs in self.unique_strings:
                assert self.unique_strings[kwargs] == job_id
            else:
                assert job_id not in self.unique_strings.values()
        finally:
            self.unique_strings[kwargs] = job_id

    @given(markus_address=st.text(), run_id=st.integers())
    def test_returns_consistent_strings(self, **kw):
        """ test that identical args return the same string """
        job_ids = {ate.format_job_id(**kw) for _ in range(10)}
        assert len(job_ids) == 1

    @given(markus_address=st.text(), 
           run_id=st.integers(), 
           kwargs=st.dictionaries(
                    st.text().filter(
                        lambda x: x not in ['markus_address', 'run_id']), 
                    st.text()))
    def test_accepts_arbitrary_kwargs(self, markus_address, run_id, kwargs):
        """ test that the function can accept arbitrary kwargs """
        ate.format_job_id(markus_address, run_id, **kwargs)

class TestCheckArgs:
    """ tests ate.check_args """

    def _generic_test(self, func, args, kwargs):
        if self._is_good(func, args, kwargs):
            try:
                ate.check_args(func, args=args, kwargs=kwargs)
            except TypeError as e:
                pytest.fail(str(e))
        else:
            with pytest.raises(TypeError):
               ate.check_args(func, args=args, kwargs=kwargs) 

    def _is_good(self, func, args, kwargs):
        try:
            inspect.signature(func).bind(*args, **kwargs)
        except TypeError:
            return False
        return True

def _make_args_kwargs(names=None):
    types = (st.text(), st.none(), st.integers())
    args = st.lists(st.one_of(*types))
    pat = f'(?:{"|".join(names)})' if names else r'[a-zA-Z]+'
    keys = st.from_regex(pat, fullmatch=True)
    values = st.one_of(*types)
    kwargs = st.dictionaries(keys, values)
    return st.tuples(args, kwargs)

for dummy_func, arg_names in dummy_functions.items():
    strategy = _make_args_kwargs(arg_names)
    @given(args_kwargs=strategy, _func=st.just(dummy_func))
    def func(self, args_kwargs, _func):
        self._generic_test(_func, *args_kwargs)
    setattr(TestCheckArgs, f'test_{dummy_func.__name__}', func)


class TestCheckForEnvironmentErrors:
    """ tests ate.check_for_environment_errors """

    def setup_method(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        file_name = 'env_creation_errors.txt'
        self.error_file = os.path.join(self.tmp_dir.name, file_name)
        with open(self.error_file, 'w') as f:
            f.write('something')

    def teardown_method(self):
        self.tmp_dir.cleanup()

    def test_reports_env_creation_error(self):
        mock_func = 'autotest_server.get_unique_env_name'
        with patch(mock_func, autospec=True, return_value=self.tmp_dir.name):
            with pytest.raises(RuntimeError) as e:
                ate.check_for_environment_errors(None, None, None)

    def test_no_report(self):
        os.unlink(self.error_file)
        mock_func = 'autotest_server.get_unique_env_name'
        with patch(mock_func, autospec=True, return_value=self.tmp_dir.name):
            try:
                ate.check_for_environment_errors(None, None, None)
            except RuntimeError as e:
                pytest.fail()

    @given(st.from_regex(r'[a-zA-Z]+', fullmatch=True))
    def test_report_contains_message(self, message):
        with open(self.error_file, 'w') as f:
            f.write(message)
        mock_func = 'autotest_server.get_unique_env_name'
        with patch(mock_func, autospec=True, return_value=self.tmp_dir.name):
            with pytest.raises(RuntimeError) as e:
                ate.check_for_environment_errors(None, None, None)
                assert message in str(e)

class TestQueueName:
    """ tests ate.queue_name """

    @classmethod
    def setup_class(cls):
        cls.unique_strings = {}

    @given(queue=st.builds(FakeQueue, st.text(min_size=1)), 
           i=st.integers())
    def test_returns_unique_strings(self, queue, i):
        """ test that unique args return unique strings """
        name = ate.queue_name(queue, i)
        try:
            if (queue.type, i) in self.unique_strings:
                assert self.unique_strings[(queue.type, i)] == name
            else:
                assert name not in self.unique_strings.values()
        finally:
            self.unique_strings[(queue.type, i)] = name

    @given(queue=st.builds(FakeQueue, st.text()), i=st.integers())
    def test_returns_consistent_strings(self, queue, i):
        """ test that identical args return the same string """
        names = {ate.queue_name(queue, i) for _ in range(10)}
        assert len(names) == 1

class TestGetQueue:
    """ tests ate.get_queue """

    @patch('autotest_server.redis_connection', autospec=True)
    @given(st.from_regex(r'[a-zA-Z]+', fullmatch=True), st.integers())
    def test_can_find_a_queue(self, redis_conn, queue_name, conn_id):
        redis_conn.return_value = conn_id
        ate.config.WORKER_QUEUES = [{'name': queue_name, 
                                     'filter': lambda **kw: True}]
        with patch('rq.Queue', FakeQueue):
            queue = ate.get_queue()
        
        assert queue.type == queue_name
        assert queue.kwargs.get('connection') == conn_id

    @patch('autotest_server.redis_connection', autospec=True)
    def test_cannot_find_a_queue(self, _redis_conn):
        ate.config.WORKER_QUEUES = [{'name': 'nothing', 
                                     'filter': lambda **kw: False}]
        with patch('rq.Queue', FakeQueue):
            with pytest.raises(RuntimeError):
                ate.get_queue()






