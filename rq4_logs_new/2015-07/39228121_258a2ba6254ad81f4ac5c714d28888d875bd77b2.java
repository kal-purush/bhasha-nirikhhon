package com.store.ws.rest;

import com.store.dao.GameDAO;
import com.store.ws.bean.GameWS;
import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Component
@Path("/game")
public class GameResource {

    @Autowired
    GameDAO gameDAO;

    @GET
    @Path("detail/{gameid}")
    @Produces(MediaType.APPLICATION_JSON)
    public GameWS getGameDetail(@PathParam("gameid") String gameId) {
        GameWS gameWS = gameDAO.findGameById(gameId);
        if (gameWS == null) {
            throw new WebApplicationException(Response.SC_NOT_FOUND);
        }
        return gameWS;
    }

}