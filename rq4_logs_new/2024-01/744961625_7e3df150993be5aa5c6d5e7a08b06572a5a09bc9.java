package uz.pdp.simline.dto.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import uz.pdp.simline.entity.PassportDetail;
import uz.pdp.simline.entity.Role;

import java.util.List;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class EmployeeUpdateDto {
    private UUID id;
    private String phoneNumber;
    private String email;
    private String gender;
    private String address;
    private Double salary;
    private boolean active;
    private PassportDetail passportDetail;
    private List<UUID> employeeRoles_id;
    private String position;
}