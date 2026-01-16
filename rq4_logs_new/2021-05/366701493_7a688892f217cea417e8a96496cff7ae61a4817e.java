package org.luukitup.webapp.controller;

import org.luukitup.webapp.dal.Blog;
import org.luukitup.webapp.dal.Project;
import org.luukitup.webapp.logic.BlogManager;
import org.luukitup.webapp.model.AddBlog;

import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/Blog")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Transactional
public class BlogController
{
    @Inject
    BlogManager blogManager;

    @GET
    public List<Blog> GetAllBlogs()
    {
        return blogManager.GetAllBlogs();
    }

    //New
    @POST
    @Path("Add")
    public Blog AddBlog(AddBlog addBlog)
    {
        return blogManager.CreateBlog(addBlog);
    }

    //Edit
    //@PUT
}