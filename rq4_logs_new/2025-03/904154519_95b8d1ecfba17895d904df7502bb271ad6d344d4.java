package it.gov.pagopa.pu.classification.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import java.util.Date;
import lombok.*;

import java.io.Serializable;

@Entity
@Table(name = "payment_notification")
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@EqualsAndHashCode(of = "paymentNotificationId", callSuper = false)
public class PaymentNotification extends BaseEntity implements Serializable{

  @Id
  private String paymentNotificationId;
  @NotNull
  private Long organizationId;
  @NotNull
  private Long ingestionFlowFileId;
  @NotNull
  private String iud;
  @NotNull
  private String iuv;
  @NotNull
  private Date paymentExecutionDate;
  @NotNull
  private String paymentType;
  @NotNull
  private Long amountPaidCents;
  private Long paCommission;
  @NotNull
  private String remittanceInformation;
  @NotNull
  private String transferCategory;
  @NotNull
  private String debtPositionTypeOrgCode;
  private String balance;
  private Long personalDataId;
  private byte[] remittanceInformationHash;
  private byte[] debtor_fiscal_code_hash;


//region keep updated semanticId
  public static String buildSemanticId(PaymentNotification paymentsReporting) {
    return paymentsReporting.getIud() + "/" +
      paymentsReporting.getOrganizationId();
  }

  private void setSemanticId() {
    this.paymentNotificationId = buildSemanticId(this);
  }

  public void setOrganizationId(Long organizationId) {
    this.organizationId = organizationId;
    setSemanticId();
  }

  public void setIud(String iud) {
    this.iud = iud;
    setSemanticId();
  }
//endregion
}