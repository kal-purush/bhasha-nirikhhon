package rest;

import com.google.gson.Gson;
import facades.PlaceFacade;
import facades.PlaceFacadeFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by adam on 01/11/2017.
 */
@Path("places")
public class PlaceResource {
    Gson gson;
    PlaceFacade facade;

    public PlaceResource() {
        this.gson = new Gson();
        facade = PlaceFacadeFactory.getInstance();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getPlaces() {
        return gson.toJson(facade.getAllPlaces());
    }
}