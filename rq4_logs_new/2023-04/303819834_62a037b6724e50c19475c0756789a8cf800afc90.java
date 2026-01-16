package ca.bc.gov.educ.api.gradstudent.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;

import java.io.Serializable;

@Data
@Entity
@Table(name = "REPORT_GRAD_DISTRICT_YE_VW")
public class ReportGradDistrictYearEndEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @jakarta.persistence.Id
    @Column(name = "MINCODE")
    private String mincode;

}