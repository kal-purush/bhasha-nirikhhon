package dev.enricosola.porcellino.response.auth;

import dev.enricosola.porcellino.response.SuccessResponse;
import dev.enricosola.porcellino.response.Response;
import java.io.Serializable;
import java.io.Serial;
import lombok.Getter;

@Getter
public class RenewResponse extends SuccessResponse implements Serializable, Response  {
    @Serial
    private static final long serialVersionUID = -3195383303406673983L;

    private final String token;

    public RenewResponse(String token){
        super(null);

        this.token = token;
    }
}