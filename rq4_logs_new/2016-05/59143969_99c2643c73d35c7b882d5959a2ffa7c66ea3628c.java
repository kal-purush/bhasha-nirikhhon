package com.bls.patronage.mapper;

import com.bls.patronage.exception.PasswordResetException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.HashMap;

@Provider
public class PasswordResetExceptionMapper implements ExceptionMapper<PasswordResetException> {

    private static final Integer defaultCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

    @Override
    public Response toResponse(PasswordResetException passwordResetException) {
        return Response.status(defaultCode)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(new HashMap<String, String>() {
                    {
                        put("code", defaultCode.toString());
                        put("message", passwordResetException.getMessage());
                    }
                }).build();
    }
}