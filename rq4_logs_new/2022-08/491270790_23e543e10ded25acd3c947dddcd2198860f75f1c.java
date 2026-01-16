package training.api.test.controller;

import io.restassured.response.Response;
import training.api.test.services.BearerService;

public class BearerController {

    private BearerService bearerService = new BearerService();

    public Response useGETMethodOnBearerURL(String authority){return this.bearerService.getMethodToBearer(authority);}
}