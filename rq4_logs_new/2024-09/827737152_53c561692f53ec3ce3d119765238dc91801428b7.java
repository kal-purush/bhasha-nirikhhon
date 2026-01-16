package mate.academy.bookstore.dto.user;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class UserLoginRequestDto {
    @NotBlank
    @Email
    @Size(min = 5, max = 30)
    private String email;
    @NotBlank
    @Size(min = 8, max = 30)
    private String password;
}