package uz.pdp.simline.dto.respone;

import lombok.AllArgsConstructor;
import lombok.Getter;
import uz.pdp.simline.entity.PassportDetail;

import java.time.LocalDate;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class PassportDetailDto {
    private UUID id;
    private String name;
    private String surname;
    private LocalDate birthDate;
    private String passportId;
    public PassportDetailDto (PassportDetail passportDetail){
        this.id = passportDetail.getId();
        this.name = passportDetail.getName();
        this.surname = passportDetail.getSurname();
        this.birthDate = passportDetail.getBirthDate();
        this.passportId = passportDetail.getPassportId();
    }
}