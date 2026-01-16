package pl.edu.pk.student.kittysecurity.dto.password;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class PasswordEntryDto {

    @JsonProperty("id")
    private Long entryId;

    @JsonProperty("name")
    private String serviceName;

    private String url;

    private String login;

    @JsonProperty("encrypted")
    private String passwordEncrypted;

    @JsonProperty("IV")
    private String Iv;
}