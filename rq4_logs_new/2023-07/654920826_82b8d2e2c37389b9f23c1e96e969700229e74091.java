package org.tms.api.adapters;

import io.restassured.response.Response;
import org.tms.api.models.Defect;

import static org.tms.utils.constants.Urls.DEFECT_URI;

public class DefectAdapter extends BaseAdapter {

    public Response getDefects(String projectCode) {
        return get(DEFECT_URI + projectCode);
    }
    public Response createDefect(String projectCode, Defect defect) {
        return post(DEFECT_URI + projectCode, gsonConverter.toJson(defect));
    }
    public Response updateDefect(String projectCode, int id, Defect defect) {
        return patch(DEFECT_URI + projectCode+ "/" + id, gsonConverter.toJson(defect));
    }
    public Response getDefect(String projectCode, int id) {
        return get(DEFECT_URI + projectCode + "/" + id);
    }
    public Response deleteDefect(String projectCode, int id) {
        return delete(DEFECT_URI + projectCode + "/" + id);
    }
}