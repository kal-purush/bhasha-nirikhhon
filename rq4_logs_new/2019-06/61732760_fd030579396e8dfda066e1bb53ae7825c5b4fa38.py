from unittest import TestCase

import responses

from ndoh_hub.utils import get_messageset_short_name, get_messageset_schedule_sequence
from ndoh_hub.utils_tests import mock_get_messageset_by_shortname, mock_get_schedule


class GetMessagesetShortnameTests(TestCase):
    def test_postbirth_batch_number(self):
        """
        The batch number should correctly correspond with the age of the baby
        """
        self.assertEqual(
            get_messageset_short_name("momconnect_postbirth", "hw_full", 0),
            "momconnect_postbirth.hw_full.1",
        )
        self.assertEqual(
            get_messageset_short_name("momconnect_postbirth", "hw_full", 14),
            "momconnect_postbirth.hw_full.1",
        )
        self.assertEqual(
            get_messageset_short_name("momconnect_postbirth", "hw_full", 15),
            "momconnect_postbirth.hw_full.2",
        )

    def test_whatsapp_postbirth_batch_number(self):
        """
        The batch number should correctly correspond with the age of the baby
        """
        self.assertEqual(
            get_messageset_short_name("whatsapp_postbirth", "hw_full", 0),
            "whatsapp_momconnect_postbirth.hw_full.1",
        )
        self.assertEqual(
            get_messageset_short_name("whatsapp_postbirth", "hw_full", 14),
            "whatsapp_momconnect_postbirth.hw_full.1",
        )
        self.assertEqual(
            get_messageset_short_name("whatsapp_postbirth", "hw_full", 15),
            "whatsapp_momconnect_postbirth.hw_full.2",
        )
        self.assertEqual(
            get_messageset_short_name("whatsapp_postbirth", "hw_full", 52),
            "whatsapp_momconnect_postbirth.hw_full.2",
        )
        self.assertEqual(
            get_messageset_short_name("whatsapp_postbirth", "hw_full", 53),
            "whatsapp_momconnect_postbirth.hw_full.3",
        )


class GetMessagesetScheduleSequenceTests(TestCase):
    @responses.activate
    def test_momconnect_postbirth_1(self):
        """
        Should calculate the correct sequence for number of weeks
        """
        schedule_id = mock_get_messageset_by_shortname("momconnect_postbirth.hw_full.1")
        mock_get_schedule(schedule_id)

        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.1", 0),
            (31, 131, 1),
        )
        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.1", 1),
            (31, 131, 3),
        )
        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.1", 14),
            (31, 131, 29),
        )

    @responses.activate
    def test_momconnect_postbirth_2(self):
        """
        Should calculate the correct sequence for number of weeks
        """
        schedule_id = mock_get_messageset_by_shortname("momconnect_postbirth.hw_full.2")
        mock_get_schedule(schedule_id)

        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.2", 15),
            (32, 132, 1),
        )
        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.2", 16),
            (32, 132, 2),
        )
        self.assertEqual(
            get_messageset_schedule_sequence("momconnect_postbirth.hw_full.2", 52),
            (32, 132, 38),
        )

    @responses.activate
    def test_whatsapp_momconnect_postbirth_1(self):
        """
        Should calculate the correct sequence for number of weeks
        """
        schedule_id = mock_get_messageset_by_shortname(
            "whatsapp_momconnect_postbirth.hw_full.1"
        )
        mock_get_schedule(schedule_id)

        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.1", 0
            ),
            (94, 194, 1),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.1", 1
            ),
            (94, 194, 3),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.1", 14
            ),
            (94, 194, 29),
        )

    @responses.activate
    def test_whatsapp_momconnect_postbirth_2(self):
        """
        Should calculate the correct sequence for number of weeks
        """
        schedule_id = mock_get_messageset_by_shortname(
            "whatsapp_momconnect_postbirth.hw_full.2"
        )
        mock_get_schedule(schedule_id)

        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.2", 15
            ),
            (97, 197, 1),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.2", 16
            ),
            (97, 197, 2),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.2", 52
            ),
            (97, 197, 38),
        )

    @responses.activate
    def test_whatsapp_momconnect_postbirth_3(self):
        """
        Should calculate the correct sequence for number of weeks
        """
        schedule_id = mock_get_messageset_by_shortname(
            "whatsapp_momconnect_postbirth.hw_full.3"
        )
        mock_get_schedule(schedule_id)

        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.3", 53
            ),
            (98, 198, 1),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.3", 54
            ),
            (98, 198, 4),
        )
        self.assertEqual(
            get_messageset_schedule_sequence(
                "whatsapp_momconnect_postbirth.hw_full.3", 104
            ),
            (98, 198, 154),
        )