import datetime
from unittest.mock import patch

import pytest

from utils.generate_canvas_assignment_data import generate_canvas_assignment_data


class TestGenerateCanvasAssignmentData:
    assignment_required_fields = [
        "due_at",
        "points_possible",
        "assignment_group_id",
        "post_to_sis",
        "graders_anonymous_to_graders",
        "grader_comments_visible_to_graders",
        "grader_names_visible_to_final_grader",
        "annotatable_attachment_id",
        "secure_params",
        "lti_context_id",
        "course_id",
        "name",
        "submission_types",
        "has_submitted_submissions",
        "due_date_required",
        "max_name_length",
        "allowed_attempts",
        "in_closed_grading_period",
        "graded_submissions_exist",
        "omit_from_final_grade",
        "hide_in_gradebook",
        "is_quiz_assignment",
        "can_duplicate",
        "original_course_id",
        "original_assignment_id",
        "workflow_state",
        "important_dates",
        "assignment_overrides",
        "publishable",
        "hidden",
        "unpublishable",
        "grading_type",
        "submission_type",
        "only_visible_to_overrides",
        "anonymous_grading",
        "description",
        "grading_standard_id",
        "allowed_extensions",
        "turnitin_enabled",
        "vericite_enabled",
        "external_tool_tag_attributes",
        "similarityDetectionTool",
        "configuration_tool_type",
        "grade_group_students_individually",
        "group_category_id",
        "peer_reviews",
        "automatic_peer_reviews",
        "peer_review_count",
        "peer_reviews_assign_at",
        "intra_group_peer_reviews",
        "anonymous_peer_reviews",
        "notify_of_update",
        "lock_at",
        "unlock_at",
        "published",
    ]

    @pytest.mark.parametrize(
        "test_today, expected_name",
        [
            (
                datetime.datetime(2020, 1, 1, 12, 00),
                "Date-01/01/20 Time-12:00",
            ),
            (
                datetime.datetime(2020, 1, 1, 12, 1),
                "Date-01/01/20 Time-12:00",
            ),
            (
                datetime.datetime(2020, 1, 1, 12, 10),
                "Date-01/01/20 Time-12:10",
            ),
            (
                datetime.datetime(2020, 1, 1, 13, 10),
                "Date-01/01/20 Time-13:10",
            ),
            (
                datetime.datetime(2020, 1, 2, 12, 00),
                "Date-02/01/20 Time-12:00",
            ),
            (
                datetime.datetime(2020, 2, 2, 12, 00),
                "Date-02/02/20 Time-12:00",
            ),
            (
                datetime.datetime(2021, 2, 2, 12, 00),
                "Date-02/02/21 Time-12:00",
            ),
        ],
    )
    @patch("utils.generate_canvas_assignment_data.datetime", wraps=datetime)
    def test_generate_canvas_assignment_data(
        self, mock_datetime, test_today, expected_name
    ):
        mock_datetime.datetime.today.return_value = test_today

        result = generate_canvas_assignment_data(
            course_id=1, assignment_group_id=1, secure_params="secure_params"
        )
        assert "assignment" in result
        assert set(result["assignment"]) == set(self.assignment_required_fields)
        assert result["assignment"]["name"] == expected_name
        assert result["assignment"]["grading_type"] == "pass_fail"
        assert "bl1nkker" in result["assignment"]["description"]
        assert result["assignment"]["published"] is True