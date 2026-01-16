from unittest import TestCase
from mock import patch, Mock, MagicMock
from forecaster import forecast


class GetForecastTest(TestCase):
    """
    Tests for get_forecast function
    """
    valid_content = {'cod': '200'}

    @classmethod
    def _valid_response(*args, **kwargs):

        response = Mock()
        response.status_code = 200
        response.content = GetForecastTest.valid_content
        response.json = Mock(return_value=GetForecastTest.valid_content)
        return response

    @patch('requests.get')
    def test_get_forecast(self, mock_requests):
        # Do some setup

        mock_requests.side_effect = GetForecastTest._valid_response

        # Run the code
        data = forecast.get_forecast('Berlin')

        # Check the response
        assert data, GetForecastTest.valid_content

    def test_non_ascii_place_name(self, mock_requests):
        pass

    def test_num_days(self, mock_requests):
        pass

    def test_units(self, mock_requests):
        pass

    @patch('requests.get')
    def test_bad_url(self, mock_requests):
        pass

    def test_not_json(self, mock_requests):
        pass

    def test_unknown_place(self, mock_requests):
        pass


class SumamriseForecastTest(TestCase):
    """
    Tests for summarise_forecast function
    """

    def test_good_args(self, mock_requests):
        # Check min, max, dates, weathers, etc
        pass