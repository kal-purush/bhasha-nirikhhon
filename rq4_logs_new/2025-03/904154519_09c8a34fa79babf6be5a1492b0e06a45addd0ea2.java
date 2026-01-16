package it.gov.pagopa.pu.classification.connector.debtposition;

import it.gov.pagopa.pu.debtposition.dto.generated.DebtPositionTypeOrg;

/**
 * Service for handling DebtPositionTypeOrg operations.
 */
public interface DebtPositionTypeOrgService {

  /**
   * Retrieves a DebtPositionTypeOrg by the given installment ID.
   *
   * @param installmentId the ID of the installment
   * @param accessToken   the access token for authentication
   * @return the DebtPositionTypeOrg associated with the given installment ID
   */
  DebtPositionTypeOrg getDebtPositionTypeOrgByInstallmentId(Long installmentId, String accessToken);

}