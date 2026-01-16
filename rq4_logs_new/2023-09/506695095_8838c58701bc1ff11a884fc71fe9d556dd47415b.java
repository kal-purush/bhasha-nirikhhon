package it.gov.pagopa.payment.instrument.dto;

import it.gov.pagopa.payment.instrument.constants.PaymentInstrumentConstants;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;


@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BaseEnrollmentBodyDTO {

  @NotBlank(message = PaymentInstrumentConstants.ERROR_MANDATORY_FIELD)
  String userId;

  @NotBlank(message = PaymentInstrumentConstants.ERROR_MANDATORY_FIELD)
  String initiativeId;

  @NotBlank(message = PaymentInstrumentConstants.ERROR_MANDATORY_FIELD)
  String channel;

  @NotBlank(message = PaymentInstrumentConstants.ERROR_MANDATORY_FIELD)
  String instrumentType;

}