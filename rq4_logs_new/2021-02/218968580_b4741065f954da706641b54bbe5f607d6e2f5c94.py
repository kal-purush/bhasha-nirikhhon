import datetime
from unittest.mock import Mock, patch

from apps.cases.models import Case
from apps.fraudprediction.models import FraudPrediction
from apps.fraudprediction.serializers import FraudPredictionSerializer
from apps.itinerary.models import ItineraryItem
from apps.visits.models import Visit
from django.urls import reverse
from model_bakery import baker
from rest_framework import status
from rest_framework.test import APITestCase
from settings.const import ISSUEMELDING

from app.utils.unittest_helpers import (
    get_authenticated_client,
    get_unauthenticated_client,
)


class PermitViewSetTest(APITestCase):
    """
    Tests for the API endpoints for retrieving case data
    """

    def test_unauthenticated_request(self):
        """
        An unauthenticated request should not be possible
        """

        url = "%s?bag_id%s" % (
            reverse("all-permits-permit checkmarks"),
            "0363010000809805",
        )
        client = get_unauthenticated_client()
        response = client.get(url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)