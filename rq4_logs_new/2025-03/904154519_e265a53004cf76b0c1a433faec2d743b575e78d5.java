package it.gov.pagopa.pu.classification.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ClassificationViewDTO {

  private Long classificationId;
  @JsonProperty("rec_fileName")
  private String receiptFileName;
  @JsonProperty("rec_numTotalRows")
  private Long receiptNumTotalRows;
  @JsonProperty("rec_iud")
  private String receiptIud;
  @JsonProperty("rec_iuv")
  private String receiptIuv;
  @JsonProperty("rec_objectVersion")
  private String receiptObjectVersion;
  @JsonProperty("rec_orgFiscalCode")
  private String receiptOrgFiscalCode;
  @JsonProperty("rec_brokerId")
  private String receiptBrokerId;
  @JsonProperty("rec_paymentReceiptId")
  private String receiptPaymentReceiptId;
  @JsonProperty("rec_paymentDateTime")
  private LocalDateTime receiptPaymentDateTime;
  @JsonProperty("rec_paymentRequestId")
  private String receiptPaymentRequestId;
  @JsonProperty("rec_paymentRequestDateTime")
  private LocalDateTime receiptPaymentRequestDateTime;
  @JsonProperty("rec_pspCodeType")
  private String receiptPspCodeType;
  @JsonProperty("rec_idPsp")
  private String receiptIdPsp;
  @JsonProperty("rec_pspCompanyName")
  private String receiptPspCompanyName;
  @JsonProperty("rec_pspOperatingUnitCode")
  private String receiptPspOperatingUnitCode;
  @JsonProperty("rec_pspOperatingUnitName")
  private String receiptPspOperatingUnitName;
  @JsonProperty("rec_pspAddress")
  private String receiptPspAddress;
  @JsonProperty("rec_pspCivic")
  private String receiptPspCivic;
  @JsonProperty("rec_pspPostalCode")
  private String receiptPspPostalCode;
  @JsonProperty("rec_pspLocation")
  private String receiptPspLocation;
  @JsonProperty("rec_pspProvince")
  private String receiptPspProvince;
  @JsonProperty("rec_pspNation")
  private String receiptPspNation;
  @JsonProperty("rec_orgEntityType")
  private String receiptOrgEntityType;
  @JsonProperty("rec_orgUniqueIdentifierCode")
  private String receiptOrgUniqueIdentifierCode;
  @JsonProperty("rec_beneficiaryOrgName")
  private String receiptBeneficiaryOrgName;
  @JsonProperty("rec_beneficiaryOrgOperatingUnitCode")
  private String receiptBeneficiaryOrgOperatingUnitCode;
  @JsonProperty("rec_beneficiaryOrgOperatingUnitName")
  private String receiptBeneficiaryOrgOperatingUnitName;
  @JsonProperty("rec_beneficiaryOrgAddress")
  private String receiptBeneficiaryOrgAddress;
  @JsonProperty("rec_beneficiaryOrgCivic")
  private String receiptBeneficiaryOrgCivic;
  @JsonProperty("rec_beneficiaryOrgPostalCode")
  private String receiptBeneficiaryOrgPostalCode;
  @JsonProperty("rec_beneficiaryOrgLocation")
  private String receiptBeneficiaryOrgLocation;
  @JsonProperty("rec_beneficiaryOrgProvince")
  private String receiptBeneficiaryOrgProvince;
  @JsonProperty("rec_beneficiaryOrgNation")
  private String receiptBeneficiaryOrgNation;
  @JsonProperty("rec_payerEntityType")
  private String payerEntityType;
  @JsonProperty("rec_payerFiscalCode")
  private String payerFiscalCode;
  @JsonProperty("rec_payerFullName")
  private String payerFullName;
  @JsonProperty("rec_payerAddress")
  private String payerAddress;
  @JsonProperty("rec_payerCivic")
  private String payerCivic;
  @JsonProperty("rec_payerPostalCode")
  private String payerPostalCode;
  @JsonProperty("rec_payerLocation")
  private String payerLocation;
  @JsonProperty("rec_payerProvince")
  private String payerProvince;
  @JsonProperty("rec_payerNation")
  private String payerNation;
  @JsonProperty("rec_payerEmail")
  private String payerEmail;
  @JsonProperty("rec_debtorEntityType")
  private String debtorEntityType;
  @JsonProperty("rec_debtorFiscalCode")
  private String debtorFiscalCode;
  @JsonProperty("rec_debtorFullName")
  private String debtorFullName;
  @JsonProperty("rec_debtorAddress")
  private String debtorAddress;
  @JsonProperty("rec_debtorCivic")
  private String debtorCivic;
  @JsonProperty("rec_debtorPostalCode")
  private String debtorPostalCode;
  @JsonProperty("rec_debtorLocation")
  private String debtorLocation;
  @JsonProperty("rec_debtorProvince")
  private String debtorProvince;
  @JsonProperty("rec_debtorNation")
  private String debtorNation;
  @JsonProperty("rec_debtorEmail")
  private String debtorEmail;
  @JsonProperty("rec_paymentOutcomeCode")
  private String receiptPaymentOutcomeCode;
  @JsonProperty("rec_paymentAmount")
  private Long receiptPaymentAmount;
  @JsonProperty("rec_creditorReferenceId")
  private String receiptCreditorReferenceId;
  @JsonProperty("rec_paymentContextId")
  private String receiptPaymentContextId;
  @JsonProperty("rec_transferAmount")
  private Long receiptTransferAmount;
  @JsonProperty("rec_transferPaymentOutcomeCode")
  private String receiptTransferPaymentOutcomeCode;
  @JsonProperty("rec_transferPaymentDateTime")
  private LocalDateTime receiptTransferPaymentDateTime;
  @JsonProperty("rec_transferPaymentReceiptId")
  private String receiptTransferPaymentReceiptId;
  @JsonProperty("rec_transferRemittanceInformation")
  private String receiptTransferRemittanceInformation;
  @JsonProperty("rec_transferCategory")
  private String receiptTransferCategory;
  @JsonProperty("rec_debtPositionTypeOrgId")
  private String receiptDebtPositionTypeOrgId;
  @JsonProperty("rec_creationDate")
  private LocalDateTime receiptCreationDate;
  @JsonProperty("rec_installmentBalance")
  private String receiptInstallmentBalance;
  @JsonProperty("payRep_objectVersion")
  private String paymentsReportingObjectVersion;
  @JsonProperty("payRep_iuf")
  private String paymentsReportingIuf;
  @JsonProperty("payRep_flowDateTime")
  private LocalDateTime paymentsReportingFlowDateTime;
  @JsonProperty("payRep_regulationUniqueIdentifier")
  private String paymentsReportingRegulationUniqueIdentifier;
  @JsonProperty("payRep_regulationDate")
  private LocalDate paymentsReportingRegulationDate;
  @JsonProperty("payRep_senderPspType")
  private String paymentsReportingSenderPspType;
  @JsonProperty("payRep_senderPspCode")
  private String paymentsReportingSenderPspCode;
  @JsonProperty("payRep_senderPspName")
  private String paymentsReportingSenderPspName;
  @JsonProperty("payRep_receiverOrganizationType")
  private String paymentsReportingReceiverOrganizationType;
  @JsonProperty("payRep_receiverOrganizationCode")
  private String paymentsReportingReceiverOrganizationCode;
  @JsonProperty("payRep_receiverOrganizationName")
  private String paymentsReportingReceiverOrganizationName;
  @JsonProperty("payRep_totalPayments")
  private Long paymentsReportingTotalPayments;
  @JsonProperty("payRep_totalAmountCents")
  private Long paymentsReportingTotalAmountCents;
  @JsonProperty("payRep_iuv")
  private String paymentsReportingIuv;
  @JsonProperty("payRep_iur")
  private String paymentsReportingIur;
  @JsonProperty("payRep_amountPaidCents")
  private Long paymentsReportingAmountPaidCents;
  @JsonProperty("payRep_paymentOutcomeCode")
  private String paymentsReportingPaymentOutcomeCode;
  @JsonProperty("payRep_payDate")
  private LocalDate paymentsReportingPayDate;
  @JsonProperty("payRep_creationDate")
  private LocalDateTime paymentsReportingCreationDate;
  @JsonProperty("tres_abiCode")
  private String treasuryAbiCode;
  @JsonProperty("tres_cabCode")
  private String treasuryCabCode;
  @JsonProperty("tres_accountRegistryCode")
  private String treasuryAccountRegistryCode;
  @JsonProperty("tres_currencyCode")
  private String treasuryCurrencyCode;
  @JsonProperty("tres_billDate")
  private LocalDate treasuryBillDate;
  @JsonProperty("tres_regionValueDate")
  private LocalDate treasuryRegionValueDate;
  @JsonProperty("tres_billAmountCents")
  private Long treasuryBillAmountCents;
  @JsonProperty("tres_signCode")
  private String treasurySignCode;
  @JsonProperty("tres_remittanceCode")
  private String treasuryRemittanceCode;
  @JsonProperty("tres_checkNumber")
  private String treasuryCheckNumber;
  @JsonProperty("tres_bankReference")
  private String treasuryBankReference;
  @JsonProperty("tres_customerReference")
  private String treasuryCustomerReference;
  @JsonProperty("tres_orderDate")
  private LocalDate treasuryOrderDate;
  @JsonProperty("tres_lastName")
  private String treasuryLastName;
  @JsonProperty("tres_orlCode")
  private String treasuryOrlCode;
  @JsonProperty("tres_iuf")
  private String treasuryIuf;
  @JsonProperty("tres_iuv")
  private String treasuryIuv;
  @JsonProperty("tres_creationDate")
  private LocalDateTime treasuryCreationDate;
  @JsonProperty("tres_billYear")
  private String treasuryBillYear;
  @JsonProperty("tres_billCode")
  private String treasuryBillCode;
  @JsonProperty("tres_domainIdCode")
  private String treasuryDomainIdCode;
  @JsonProperty("tres_receptionDate")
  private LocalDate treasuryReceptionDate;
  @JsonProperty("tres_documentYear")
  private String treasuryDocumentYear;
  @JsonProperty("tres_documentCode")
  private String treasuryDocumentCode;
  @JsonProperty("tres_provisionalAe")
  private String treasuryProvisionalAe;
  @JsonProperty("tres_provisionalCode")
  private String treasuryProvisionalCode;
  @JsonProperty("tres_actualSuspensionDate")
  private LocalDate treasuryActualSuspensionDate;
  @JsonProperty("tres_managementProvisionalCode")
  private String treasuryManagementProvisionalCode;
  @JsonProperty("classificationLabel")
  private String classificationLabel;
  @JsonProperty("classificationDate")
  private LocalDate classificationDate;
  @JsonProperty("typeCodePal")
  private String typeCodePal;
  @JsonProperty("typeDescriptionPal")
  private String typeDescriptionPal;
  @JsonProperty("taxonomicCodePal")
  private String taxonomicCodePal;
  @JsonProperty("fiscalCodePal")
  private String fiscalCodePal;
  @JsonProperty("namePal")
  private String namePal;
}