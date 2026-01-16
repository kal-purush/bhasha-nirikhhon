"""
Hamcrest custom matchers used by unit-tests.

"""
from hamcrest.core.base_matcher import BaseMatcher
from urltools import normalize


class URLMatcher(BaseMatcher):
    """
    Hamcrest matcher for comparing URLs by canonicalizing them first.

    Example:

        with graph.app.test_request_context():
           assert_that(url, matches_url("https://canonical.url.com/path?a=1&b=2"))

    """
    def __init__(self, url):
        self.url = url

    def _matches(self, item):
        return normalize(item) == normalize(self.url)

    def describe_to(self, description):
        description.append_text("expected URL: {}".format(self.url))


def matches_url(url):
    return URLMatcher(url)