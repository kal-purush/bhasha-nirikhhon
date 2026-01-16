package it.gov.pagopa.pu.debtpositions.controller;

import it.gov.pagopa.pu.debtpositions.controller.generated.ReceiptApi;
import it.gov.pagopa.pu.debtpositions.dto.generated.ReceiptDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceiptControllerImpl implements ReceiptApi {
  @Override
  public ResponseEntity<ReceiptDTO> createReceipt(ReceiptDTO receiptDTO) {
    return ReceiptApi.super.createReceipt(receiptDTO);
  }
}