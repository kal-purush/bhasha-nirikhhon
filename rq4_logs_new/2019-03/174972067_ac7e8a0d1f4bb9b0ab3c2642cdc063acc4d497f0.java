package nl.sean.dea.exceptionmapper;

import nl.sean.dea.dto.ErrorDTO;
import nl.sean.dea.service.SpotitubeAuthenticationException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class LoginExceptionMapper implements ExceptionMapper<SpotitubeAuthenticationException> {

    @Override
    public Response toResponse(SpotitubeAuthenticationException exception) {
        return Response.status(Response.Status.UNAUTHORIZED).entity(new ErrorDTO(exception.getMessage())).build();
    }
}