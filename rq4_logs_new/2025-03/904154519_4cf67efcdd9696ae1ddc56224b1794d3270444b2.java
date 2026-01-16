package it.gov.pagopa.pu.classification.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.io.Serializable;

@Entity
@Table(name = "assessments")
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@EqualsAndHashCode(callSuper = false)
public class Assessments extends BaseEntity implements Serializable {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "assessments_generator")
  @SequenceGenerator(name = "assessments_generator", sequenceName = "assessment_id_seq", allocationSize = 1)
  private Long assessmentId;
  @NotNull
  private Long organizationId;
  @NotNull
  private String debtPositionTypeOrgCode;
  @NotNull
  private String status;
  @NotNull
  private String assessmentName;
  @NotNull
  private boolean printed;
}