package com.jpaul;

import com.jpaul.dao.impl.DAOManager;
import com.jpaul.model.Category;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
public class MyResource {
    DAOManager daoManager = new DAOManager();

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */

    /*
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return "Got it!";
    }
    */
    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getVar(@PathParam("id") String id){
        return id;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Category> categories()throws Exception{
        return daoManager.getCategoryDAO().read();
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Category create(Category category)throws Exception{
        return daoManager.getCategoryDAO().create(category);
    }





}