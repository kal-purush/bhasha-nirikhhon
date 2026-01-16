package org.tms.api.adapters;

import io.restassured.response.Response;
import org.tms.api.models.Case;

import static org.tms.utils.constants.Urls.CASE_URI;

public class CaseAdapter extends BaseAdapter {
    public Response getCases(String projectCode) {
        return get(CASE_URI + projectCode);
    }
    public Response getCaseById(String projectCode, int caseId) {
        return get(CASE_URI + projectCode + "/" + caseId);
    }
    public Response createCase(String projectCode, Case testCase) {
        return post(CASE_URI + projectCode, gsonConverter.toJson(testCase));
    }
    public Response deleteCase(String projectCode, int caseId) {
        return delete(CASE_URI + projectCode + "/" + caseId);
    }
}