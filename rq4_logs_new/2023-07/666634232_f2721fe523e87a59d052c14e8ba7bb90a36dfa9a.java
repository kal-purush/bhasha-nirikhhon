package groupone.userservice.dto.request;

import lombok.*;

import javax.validation.constraints.NotNull;

@Data
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
public class RegisterRequest {

    @NotNull(message = "First name is required")
    private String firstName;

    @NotNull(message = "Last name is required")
    private String lastName;

    @NotNull(message = "Email is required")
    private String email;

    @NotNull(message = "Password is required")
    private String password;
}