package com.templetree.exception;


import com.templetree.model.ErrorMessage;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * @author Lalith Mannur
 */

@Provider
public class TempletreeExceptionMapper implements javax.ws.rs.ext.ExceptionMapper<TempletreeException> {

    @Override
    public Response toResponse(TempletreeException ex) {
        return Response.status(ex.getStatus())
                .entity(new ErrorMessage(ex))
                .type(MediaType.APPLICATION_JSON)
                .build();
    }
}