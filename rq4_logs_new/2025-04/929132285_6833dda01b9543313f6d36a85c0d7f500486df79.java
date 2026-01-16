package nl.optifit.backendservice.dto;

import lombok.Getter;
import lombok.Setter;
import nl.optifit.backendservice.model.Account;
import nl.optifit.backendservice.model.Biometrics;

import java.time.LocalDateTime;

@Getter
@Setter
public class BiometricsMeasurementDTO {
    private Double weight;
    private Double fat;
    private Integer visceralFat;

    public static Biometrics toBiometrics(Account account, BiometricsMeasurementDTO biometricsMeasurementDTO) {
        return Biometrics.builder()
                .account(account)
                .measuredOn(LocalDateTime.now())
                .weight(biometricsMeasurementDTO.getWeight())
                .fat(biometricsMeasurementDTO.getFat())
                .visceralFat(biometricsMeasurementDTO.getVisceralFat())
                .build();
    }
}