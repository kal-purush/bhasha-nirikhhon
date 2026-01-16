package it.gov.pagopa.pu.debtpositions.model.debtpositiontype;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "debt_position_type")
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class DebtPositionTypeWithCount implements Serializable {

  @Id
  private Long debtPositionTypeId;
  private String code;
  private String description;
  private LocalDateTime updateDate;
  private Integer activeOrganizations;
}