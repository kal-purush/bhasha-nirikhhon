from _pytest.config import get_config
from unit_testing.unit_testing import CountError, count_letter_frequency as clf
import pytest


class TestCountLetterFrequency(object):

    # we can also use yeild if we are following the setup, assertion, teardown workflow. Setup is done before yield, and teardown is after yeild
    # Here the context manager is actually handeling all of these together, it opens the file, reads its content, closes the file
    @pytest.fixture
    def get_content(self):
        with open('./assets/test.txt') as f:
            return f.read()
    # test return type

    def test_count_letter_returns_an_integer_if_letter_exists(self, get_content):
        expected = int
        actual = clf(get_content, 'a')
        assert isinstance(actual, expected)
    # test returned values

    def test_count_letter_returns_correct_count(self, get_content):
        expected = 4
        actual = clf(get_content, 'a')
        assert expected == actual
    # test exceptions

    def test_count_frequency_raises_a_CountError_exception_if_letter_is_not_found_in_content(self, get_content):
        with pytest.raises(CountError) as cr:
            clf(get_content, 'f')
        assert cr.match('Letter cannot be found in the content.')
    # skip the test if the condition passed to skipif evaluates to True. Here, I am skipping the test if the content is not empty (not ideal but you get the idea :))

    @pytest.mark.skipif(get_content != '', reason='Content is empty')
    def test_count_frequency_raises_a_CountError_exception_if_content_is_empty(self):
        with pytest.raises(CountError) as cr:
            clf('', 'f')
        assert cr.match('Letter cannot be found in the content.')
    # skip a test especially while something is in development

    @pytest.mark.skip("in progress")
    def test_count_frequency_returns_True_if_the_letter_is_the_most_frequent(slef, get_content):
        expected = True
        actual = clf(get_content, 'a')
        assert expected == actual
    # mark as xfail, which means that before the actual implementation, it is expected that this test will fail since this test is considered as a false alarm, so we are telling pytest that this test is expected to fail

    @pytest.mark.xfail()
    def test_count_frequency_returns_correct_message_if_frequency_is_higher_than_100(self, get_content):
        expected = 'Frequency is above 100'
        actual = clf(get_content, 'z')
        assert expected == actual

    