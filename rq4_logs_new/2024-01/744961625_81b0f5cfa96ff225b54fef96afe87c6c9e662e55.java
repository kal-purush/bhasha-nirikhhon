package uz.pdp.simline.dto.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class CustomerUpdateDto{
    private UUID id;
    private String username;
    private String email;
    private String phoneNumber;
    private String password;
    PassportDetailDto passportDetail;
    @AllArgsConstructor
    @Getter
    public static class PassportDetailDto{
        private String name;
        private String surname;
        private LocalDate birthDate;
        private String passportId;
    }
}