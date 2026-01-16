package fa.training.dto;

import fa.training.enums.CurrentPosition;
import fa.training.enums.HighestLevel;
import fa.training.enums.Skills;
import fa.training.enums.Status;
import jakarta.validation.constraints.*;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Getter
@Setter
public class CandidateDTO {
    private Long candidateId;

    @NotBlank(message = "Full name is required")
    private String fullname;

    @Past(message = "Date of birth must be in the past")
    private Date dob;

    private String phone;

    @Email(message = "Email should be valid")
    @NotBlank(message = "Email is required")
    private String email;

    private String address;

    @NotNull(message = "Gender is required")
    private Boolean gender;

    private String attachment;

    @NotNull(message = "At least one skill is required")
    private List<String> skills = new ArrayList<>();

//    private String skillsAsString;

//    @NotNull(message = "At least one skill is required")
//    private List<Skills> skills = new ArrayList<>();

    @NotNull(message = "Position is required")
    private CurrentPosition position;

    private String note;

    @NotNull(message = "Status is required")
    private Status status;

    @Min(value = 0, message = "Years of experience must be 0 or more")
    private Integer yearOfExp;

    @NotNull(message = "Highest level is required")
    private HighestLevel highestLevel;

}
