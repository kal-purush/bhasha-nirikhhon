package com.thoughtworks.learning.api;

import com.thoughtworks.learning.core.Items;
import com.thoughtworks.learning.core.ItemsRepository;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Created by julie on 12/30/15.
 */
@Path("/items")
public class ItemsResource {
    @Inject
    private ItemsRepository itemsRepository;
    @Path("/")
    @GET
    @Produces(MediaType.APPLICATION_JSON+";charset=utf-8")
    public Response list(){
        List<Items> list = itemsRepository.findItems();
        return Response.status(200).entity(list).build();
    }

}