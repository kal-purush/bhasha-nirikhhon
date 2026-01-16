package it.gov.pagopa.pu.debtpositions.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.gov.pagopa.pu.debtpositions.model.InstallmentNoPII;
import it.gov.pagopa.pu.debtpositions.model.Transfer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Installment {

  private Long installmentId;
  private Long paymentOptionId;
  private String status;
  private String iupdPagopa;
  private String iud;
  private String iuv;
  private String iur;
  private String iuf;
  private OffsetDateTime dueDate;
  private String paymentTypeCode;
  private Long amountCents;
  private Long notificationFeeCents;
  private String remittanceInformation;
  private String humanFriendlyRemittanceInformation;
  private String balance;
  private String legacyPaymentMetadata;
  private Person debtor;
  private List<Transfer> transfers;
  private OffsetDateTime creationDate;
  private OffsetDateTime updateDate;
  private Long updateOperatorExternalId;

  @JsonIgnore
  private InstallmentNoPII noPII;
}