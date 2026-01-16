package dev.enricosola.porcellino.response.user;

import dev.enricosola.porcellino.response.SuccessResponse;
import dev.enricosola.porcellino.response.Response;
import dev.enricosola.porcellino.dto.UserDTO;
import java.io.Serializable;
import java.io.Serial;
import lombok.Getter;


@Getter
public class UserInfoResponse extends SuccessResponse implements Response, Serializable {
    @Serial
    private static final long serialVersionUID = 5060317856163010263L;

    protected final UserDTO user;

    public UserInfoResponse(UserDTO user){
        super(null);

        this.user = user;
    }
}