package org.example.atharvolunteeringplatform.DTO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.example.atharvolunteeringplatform.Model.Opportunity;

import java.time.LocalDate;

@Getter
@Setter
@RequiredArgsConstructor
public class OpportunityDTO {//لاظهار معلومات محدوده للطالب

    private Integer id;
    private String title;
    private String typeOpportunity;
    private String gender;
    private String description;
    private LocalDate startDate;
    private LocalDate endDate;
    private Integer hours;
    private Integer studentCapacity;
    private String location;

    public OpportunityDTO(Opportunity opportunity) {
        this.id = opportunity.getId();
        this.title = opportunity.getTitle();
        this.typeOpportunity = opportunity.getTypeOpportunity();
        this.gender = opportunity.getGender();
        this.description = opportunity.getDescription();
        this.startDate = opportunity.getStartDate();
        this.endDate = opportunity.getEndDate();
        this.hours = opportunity.getHours();
        this.studentCapacity = opportunity.getStudentCapacity();
        this.location = opportunity.getLocation();
    }}