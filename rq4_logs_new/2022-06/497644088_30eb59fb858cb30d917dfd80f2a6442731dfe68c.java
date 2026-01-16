package controller;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import service.StateService;

@Path("/state")
public class StateController {

    @Inject
    StateService stateService;

    /**
     * Endpoint de test
     * 
     * @param nbLetters
     * @return
     */
    @GET
    @Path("/word/{nbLetters}")
    public Response getRandomWord(@PathParam("nbLetters") int nbLetters) {
        return Response.ok(Map.of("word", stateService.getRandomWord(nbLetters))).build();
    }
}