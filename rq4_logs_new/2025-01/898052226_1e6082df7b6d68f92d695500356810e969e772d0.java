package it.gov.pagopa.pu.debtpositions.model;

import it.gov.pagopa.pu.debtpositions.dto.generated.InstallmentStatus;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Embeddable
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class InstallmentSyncStatus implements Serializable {

  @Enumerated(EnumType.STRING)
  private InstallmentStatus syncStatusFrom;
  @Enumerated(EnumType.STRING)
  private InstallmentStatus syncStatusTo;

}