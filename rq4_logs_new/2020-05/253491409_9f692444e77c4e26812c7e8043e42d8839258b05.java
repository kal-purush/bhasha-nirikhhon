package org.dontcode.ide;

import org.dontcode.ide.preview.PreviewServiceClient;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/test")
public class IdeTestResource {
    private static Logger log = LoggerFactory.getLogger(IdeTestResource.class);

    @Inject
    @RestClient
    PreviewServiceClient previewServiceClient;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response testAsIde(String update) {
        log.debug("Receiving from ide test {}", update);
        previewServiceClient.receiveUpdate(update);
        return Response.ok().build();
    }
}