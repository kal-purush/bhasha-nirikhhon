package pl.edu.pk.student.kittysecurity.dto.user;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Email;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
@AllArgsConstructor
public class UserUpdateResponseDto {

    private String status;

    @JsonProperty("username")
    private String displayName;

    private String email;

    @JsonProperty("updated_at")
    private long updatedAt;
}