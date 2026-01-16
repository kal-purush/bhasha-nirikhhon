package com.stokmate.backend;

import com.google.api.server.spi.config.Api;
import com.google.api.server.spi.config.ApiMethod;
import com.google.api.server.spi.config.ApiNamespace;

import javax.inject.Named;

@Api(
        name = "sm",
        version = "v1",
        namespace = @ApiNamespace(
                ownerDomain = "backend.stokmate.com",
                ownerName = "backend.stokmate.com",
                packagePath = ""
        )
)
public class ApiEndpoint {

    @ApiMethod(
            name = "groups.get",
            path = "groups/{id}",
            httpMethod = ApiMethod.HttpMethod.GET
    )
    public Group getGroup(@Named("id") long id) {
        Group response = new Group();
        response.setId(id);
        response.setName("Group1");
        response.setAdmin("Admin1");
        response.setStatus(0);
        response.setMessage("SUCCESS");
        return response;
    }

    @ApiMethod(
            name = "groups.createOrUpdate",
            path = "groups",
            httpMethod = ApiMethod.HttpMethod.POST
    )
    public Response createOrUpdateGroup(Group group) {
        Response response = new Response();
        response.setStatus(0);
        response.setMessage("SUCCESS");
        return response;
    }

    @ApiMethod(
            name = "groups.delete",
            path = "groups/{id}",
            httpMethod = ApiMethod.HttpMethod.DELETE
    )
    public Response deleteGroup(@Named("id") long id) {
        Response response = new Response();
        response.setStatus(0);
        response.setMessage("SUCCESS");
        return response;
    }
}