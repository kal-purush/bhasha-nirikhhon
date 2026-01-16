package groupone.userservice.dto.request;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.RequestParam;

@Data
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserPatchRequest {
//    @NotBlank(message = "email cannot be blank")
    String email;
//    @NotBlank(message = "first name cannot be blank")
    String firstName;
//    @NotBlank(message = "last name cannot be blank")
    String lastName;
//    @NotBlank(message = "password cannot be blank")
    String password;
    String profileImageURL;
//    Integer type;
}