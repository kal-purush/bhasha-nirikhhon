package lk.zerocode.api.controller.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.validation.constraints.NotBlank;
import lk.zerocode.api.model.Employee;
import lk.zerocode.api.model.Status;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalTime;


@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class AttendanceDTO {

    @NotBlank
    private LocalDate date;

    private LocalTime actualCheckIn;

    private LocalTime actualCheckOut;

    private LocalTime requiredCheckIn;

    private LocalTime requiredCheckOut;

    private String dayType;//morning/evining

    @Enumerated(EnumType.STRING)
    private Status status;

    @NotBlank
    private Long employeeId;
}