package fr.esir.jxc.events;
import java.util.UUID;

import fr.esir.jxc.DTO.UserCreationDTO;
import fr.esir.jxc.utils.Hash;
import lombok.Value;

@Value
public class UserCreated {

    String id;
    String username;
    String password;
    String email;

    public static UserCreated of(UserCreationDTO user) {
        return new UserCreated(
                UUID.randomUUID().toString(),
                user.getUsername(),
                Hash.hash(user.getPassword()),
                user.getEmail()
        );
    }

}