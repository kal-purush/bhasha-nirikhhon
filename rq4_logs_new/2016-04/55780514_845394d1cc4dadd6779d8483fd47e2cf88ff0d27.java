package ar.gov.santafe.meduc.interfaces;

import ar.gov.santafe.meduc.dto.SimpleDto;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author enorrmann
 */
public interface GenericService {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<SimpleDto> all();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public SimpleDto getById(@PathParam("id")String id);

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SimpleDto create(SimpleDto simpleDto);

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SimpleDto update(@PathParam("id") String id, SimpleDto simpleDto);

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    public SimpleDto delete(@PathParam("id") String id);

}