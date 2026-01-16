package it.gov.pagopa.pu.classification.connector.debtposition;

import it.gov.pagopa.pu.debtposition.dto.generated.InstallmentNoPIIResponse;

import java.util.List;

/**
 * Service for handling InstallmentNoPII operations.
 */
public interface InstallmentNoPIIService {

  /**
   * Finds a list of InstallmentNoPII by the given receipt ID.
   *
   * @param receiptId the unique identifier of the receipt.
   * @return a list of InstallmentNoPII associated with the given receipt ID.
   */
  List<InstallmentNoPIIResponse> getByReceiptId(Long receiptId, String accessToken);

}