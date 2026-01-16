package api.adapters;

import api.models.Project;
import io.qameta.allure.Step;
import io.restassured.response.Response;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProjectAdapter extends BaseAdapter {
    public final String PROJECT_URL = "/project";
    public final String SPECIFIC_PROJECT_URL = "/project/%s";

    @Step("Create new project with project code: '{projectCode}'")
    public Response createProject(Project project) {
        log.info(String.format("Create new '%s' project", project));
        return post(PROJECT_URL, converter.toJson(project)).then().log().all().extract().response();
    }

    @Step("Get '{projectCode}' project")
    public Response getProjectByCode(String projectCode) {
        log.info(String.format("Get '%s' project.", projectCode));
        return new BaseAdapter().get(String.format(SPECIFIC_PROJECT_URL, projectCode));
    }

    @Step("Delete project by code:'{projectCode}'")
    public Response deleteProjectByCode(String projectCode){
        log.info(String.format("Delete '%s' project", projectCode));
        return new BaseAdapter().delete(String.format(SPECIFIC_PROJECT_URL, projectCode));

    }


}