package it.gov.pagopa.pu.debtpositions.controller;

import it.gov.pagopa.pu.debtpositions.controller.generated.TransferApi;
import it.gov.pagopa.pu.debtpositions.dto.generated.DebtPositionDTO;
import it.gov.pagopa.pu.debtpositions.service.statusalign.DebtPositionHierarchyStatusAlignerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransferControllerImpl implements TransferApi {

  private final DebtPositionHierarchyStatusAlignerService debtPositionHierarchyStatusAlignerService;

  public TransferControllerImpl(DebtPositionHierarchyStatusAlignerService debtPositionHierarchyStatusAlignerService) {
    this.debtPositionHierarchyStatusAlignerService = debtPositionHierarchyStatusAlignerService;
  }

  @Override
  public ResponseEntity<DebtPositionDTO> notifyReportedTransferId(Long transferId){
    DebtPositionDTO body = debtPositionHierarchyStatusAlignerService.notifyReportedTransferId(transferId);
    return new ResponseEntity<>(body, HttpStatus.OK);
  }
}