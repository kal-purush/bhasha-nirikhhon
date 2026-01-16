package lk.zerocode.api.controller.dto;

import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lk.zerocode.api.model.Gender;
import lombok.Data;
import lombok.NonNull;

import java.time.LocalDate;

@Data
public class BasicDetailsDTO {

    private Long id;
    private String first_name;
    private String last_name;
    private LocalDate dob;
    private String address;
    private String contact_number;
    private String email;
    private String image_path;
    private String nic;
    private String work_telephone;

    @Enumerated(EnumType.STRING)
    private Gender gender;
}