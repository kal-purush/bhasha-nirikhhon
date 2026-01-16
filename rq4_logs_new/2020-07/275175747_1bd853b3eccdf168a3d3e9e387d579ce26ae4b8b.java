package jm.stockx.dto;

import jm.stockx.entity.User;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class UserLoginDTO {

    private String email;

    private String password;

    private Boolean rememberMe;

    public UserLoginDTO(User user) {
        this.email = user.getEmail();
        this.password = user.getPassword();
    }

    public UserLoginDTO(UserDTO userDTO) {
        this.email = userDTO.getEmail();
        this.password = userDTO.getPassword();
    }
}