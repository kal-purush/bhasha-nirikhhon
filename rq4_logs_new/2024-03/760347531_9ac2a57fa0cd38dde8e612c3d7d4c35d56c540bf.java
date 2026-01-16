package lk.zerocode.api.controller.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lk.zerocode.api.model.Status;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.Year;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class OtherLeavesDTO {

    private String department;
    private String leaveType;
    private String dayType;
    private String reason;
    private Year financialYear;
    private Month financialMonth;



    private Status status;
    private Float hours;
    //    private MonthlyBasedLeave monthlyBasedLeaves;
//    private FingerPrint fingerPrint;
    private LocalTime wantedTime;
    private LocalDate wantedDate;

    
}