package pl.edu.pk.student.kittysecurity.dto.user;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class UserUpdateRequestDto {

    @JsonProperty("username")
    @Size(min = 4, message = "Username should be at least 4 characters long")
    private String displayName;

    @JsonProperty("master_hash")
    private String masterHash;

    @Email(message = "Email should be valid!")
    private String email;

}