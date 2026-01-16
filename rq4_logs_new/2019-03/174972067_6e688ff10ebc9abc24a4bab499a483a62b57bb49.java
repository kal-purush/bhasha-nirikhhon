package nl.sean.dea.exceptionmapper;

import nl.sean.dea.dto.ErrorDTO;
import nl.sean.dea.service.SpotitubeTokenException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class TokenExceptionMapper implements ExceptionMapper<SpotitubeTokenException> {
    @Override
    public Response toResponse(SpotitubeTokenException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorDTO(e.getMessage())).build();
    }
}