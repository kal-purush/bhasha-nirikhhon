import jsonpickle
from flask import Blueprint, send_file
from flask import current_app as app
from ingest.api.ingestapi import IngestApi
import io

from broker.service.spreadsheet_storage.spreadsheet_storage_exceptions import SubmissionSpreadsheetDoesntExist
from broker.service.spreadsheet_storage.spreadsheet_storage_service import SpreadsheetStorageService
from broker.service.summary_service import SummaryService

ingest_api = IngestApi()

submissions_bp = Blueprint(
    'submissions', __name__,
)


@submissions_bp.route('/<submission_uuid>/spreadsheet', methods=['GET'])
def export_to_spreadsheet():
    return f'not implemented'


@submissions_bp.route('/<submission_uuid>/spreadsheet/original', methods=['GET'])
def get_submission_spreadsheet(submission_uuid):
    try:
        spreadsheet = SpreadsheetStorageService(app.SPREADSHEET_STORAGE_DIR).retrieve(submission_uuid)
        spreadsheet_name = spreadsheet["name"]
        spreadsheet_blob = spreadsheet["blob"]

        return send_file(
            io.BytesIO(spreadsheet_blob),
            mimetype='application/octet-stream',
            as_attachment=True,
            attachment_filename=spreadsheet_name)
    except SubmissionSpreadsheetDoesntExist as e:
        return app.response_class(
            response={"message": f'No spreadsheet found for submission with uuid {submission_uuid}'},
            status=404,
            mimetype='application/json'
        )


@submissions_bp.route('/<submission_uuid>/summary', methods=['GET'])
def submission_summary(submission_uuid):
    submission = ingest_api.get_submission_by_uuid(submission_uuid)
    summary = SummaryService().summary_for_submission(submission)

    return app.response_class(
        response=jsonpickle.encode(summary, unpicklable=False),
        status=200,
        mimetype='application/json'
    )