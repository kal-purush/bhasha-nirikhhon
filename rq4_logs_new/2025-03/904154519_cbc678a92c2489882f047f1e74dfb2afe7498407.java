package it.gov.pagopa.pu.classification.model.view;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

@Entity
@Table(name = "treasury")
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class TreasuryView implements Serializable {

  @Id
  private String treasuryId;
  private Long organizationId;
  private String billYear;
  private String billCode;
  private LocalDate regionValueDate;
  private LocalDate billDate;
  private String iuf;
  private Long billAmountCents;
  private String iuv;
  private String provisionalCode;
  private String pspLastName;
  private String documentCode;

}