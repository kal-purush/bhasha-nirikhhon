import pytest
from time import sleep

from test.conftest import is_dev_env

from bfex.models import Faculty, Keywords
from bfex.components.search_engine import builder, parser
from bfex.blueprints.search_api import SearchAPI


@pytest.mark.skipif(is_dev_env(), reason="Not running in build environment.")
class TestSearchResults(object):
    query_builder = builder.QueryBuilder()
    query_parser = parser.QueryParser()

    @classmethod
    def setup_class(cls):
        Keywords.search().delete()
        Faculty.search().delete()
        sleep(3)

        will = Faculty(meta={"id": 379}, name="William.Allison", full_name="Allison, William.",
                       faculty_id=379, email="william@ualberta.ca")
        will.save()
        will_keywords = Keywords(faculty_id=379, datasource="test", approach_id=0,
                                 keywords=["zebrafish", "evolutionary processes", "marine"])
        will_keywords.save()

        vince = Faculty(meta={"id": 356}, name="Vincent.Bouchard", full_name="Bouchard, Vincent",
                        faculty_id=356, email="vincent@ualberta.ca")
        vince.save()
        vince_keywords = Keywords(faculty_id=356, datasource="test", approach_id=0,
                                  keywords=["string theory", "number theory", "mathematics"])
        vince_keywords.save()

        sleep(3)

    def test_simple_search(self):
        """Test the results of queries on actual data."""
        query = "zebrafish"
        elastic_query = self.build_query(query)
        results = Keywords.search().query(elastic_query).execute()
        assert len(results) == 1

        query = "zebrafish OR mathematics"
        elastic_query = self.build_query(query)
        results = Keywords.search().query(elastic_query).execute()
        assert len(results) == 2

        query = "zebrafish AND mathematics"
        elastic_query = self.build_query(query)
        results = Keywords.search().query(elastic_query).execute()
        assert len(results) == 0

    def test_add_name_to_search_results(self):
        faculty_with_keywords = {}
        query = "zebrafish"
        pf_query = self.query_parser.parse_query(query)
        SearchAPI.add_name_search_results(faculty_with_keywords, pf_query)
        assert len(faculty_with_keywords) == 0

        faculty_with_keywords = {}
        query = "\"Allison, William\""
        pf_query = self.query_parser.parse_query(query)
        SearchAPI.add_name_search_results(faculty_with_keywords, pf_query)
        assert len(faculty_with_keywords) == 1
        assert 379 in faculty_with_keywords

        faculty_with_keywords = {}
        query = "\"Allison, William\" OR \"Bouchard, Vincent\""
        pf_query = self.query_parser.parse_query(query)
        SearchAPI.add_name_search_results(faculty_with_keywords, pf_query)
        assert len(faculty_with_keywords) == 2
        assert 379 in faculty_with_keywords
        assert 356 in faculty_with_keywords

        faculty_with_keywords = {}
        query = "\"Allison, William\" AND \"Bouchard, Vincent\""
        pf_query = self.query_parser.parse_query(query)
        SearchAPI.add_name_search_results(faculty_with_keywords, pf_query)
        assert len(faculty_with_keywords) == 0

    def build_query(self, query, search_field="keywords"):
        """Utility function to parse and build an elasticsearch query."""
        pf_query = self.query_parser.parse_query(query)
        return self.query_builder.build(pf_query, search_field=search_field)

    @classmethod
    def teardown_class(cls):
        Faculty.get(id=379).delete()
        Faculty.get(id=356).delete()

        Keywords.search().query('match', faculty_id=379).delete()
        Keywords.search().query('match', faculty_id=356).delete()
