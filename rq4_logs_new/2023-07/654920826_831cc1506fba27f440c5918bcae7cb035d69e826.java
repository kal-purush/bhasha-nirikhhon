package org.tms.api.tests;

import io.restassured.response.Response;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.tms.api.adapters.ProjectAdapter;
import org.tms.api.adapters.SuiteAdapter;
import org.tms.api.models.Project;
import org.tms.api.models.Suite;
import org.tms.utils.tools.TestDataGenerator;

public class ProjectWithSuiteTest {
    private String projectCode;
    private int suiteId;
    @Test
    public void createProjectTest() {
        projectCode = TestDataGenerator.createProjectCode();
        Project project = Project.builder()
                .title(TestDataGenerator.createProjectName())
                .code(projectCode)
                .build();
        String projectCodeFromResponse = new ProjectAdapter().createProject(project).body().path("result.code");
        Assert.assertEquals(projectCodeFromResponse, project.getCode(), "POST /project failed!");
    }

    @Test(dependsOnMethods = "createProjectTest")
    public void createSuiteTest() {
        Suite suite = Suite.builder()
                .title(TestDataGenerator.createSuiteTitle())
                .description(TestDataGenerator.createSuiteDescription())
                .build();
        Response response = new SuiteAdapter().createSuite(projectCode, suite);

        suiteId = response.body().path("result.id");
        boolean responseStatus = response.body().path("status");

        Assert.assertTrue(responseStatus, "POST /suite/"+ projectCode + " failed!");
    }
    @Test(dependsOnMethods = "createSuiteTest")
    public void deleteSuiteTest() {
        boolean responseStatus = new SuiteAdapter().deleteSuite(projectCode,suiteId).body().path("status");
        Assert.assertTrue(responseStatus, "DELETE /suite/" + projectCode + "/" + suiteId + " failed!");
    }
    @Test(dependsOnMethods = "createProjectTest", priority = 1)
    public void deleteProjectTest() {
        boolean responseStatus = new ProjectAdapter().deleteProject(projectCode).body().path("status");
        Assert.assertTrue(responseStatus, "DELETE /project/" + projectCode + " failed!");
    }
}