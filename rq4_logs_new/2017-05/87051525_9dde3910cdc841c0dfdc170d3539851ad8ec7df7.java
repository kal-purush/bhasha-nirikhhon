/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bcferries.apps.payment.client.ebooking.junit;
// Note on import: make sure to use only com.bcferries.apps.payment.client.api part of package
import com.bcferries.apps.payment.client.api.*;
import java.math.BigInteger;
  
  
public class CallingPaymentServiceSample {
  
    public static void main(String[] args) {
    PaymentServiceFacade facade = new PaymentServiceFacade();
    // get payment service version
    GetVersionRequest getVersionRequest = new GetVersionRequest();
    GetVersionResponse getVersionResponse;
    getVersionResponse = facade.getVersion(null);
    String ver = getVersionResponse.getVersion();
    System.out.println("BCF payment service version: " + ver);
  
    ProcessPaymentRequest processPaymentRequest = new ProcessPaymentRequest();
    ProcessPaymentResponse processPaymentResponse;
  
    // Set source system info
    SourceSystemInfo sourceSystemInfo = new SourceSystemInfo();
    sourceSystemInfo.setAppliId("EBOOKING");
    sourceSystemInfo.setLocationId("SWB");
    sourceSystemInfo.setStationId("BCFStationID");
    sourceSystemInfo.setPassword("password123!");
    processPaymentRequest.setSourceSystemInfo(sourceSystemInfo);
  
    // Prcoess payment
    // Set card info request
    CardInfoRequest cardInfoRequest = new CardInfoRequest();
    cardInfoRequest.setExpectedCardType(ExpectedCardType.CREDIT_DEBIT);
    //cardInfoRequest.setExpectedCardType(CardType.BCF_CARD);  //if BCF card
    processPaymentRequest.setCardInfoRequest(cardInfoRequest);
  
    // Set transaction info request
    TransactionInfoRequest transactionInfoRequest = new TransactionInfoRequest();
    transactionInfoRequest.setTransactionType(TransactionType.PURCHASE);
  
    //transactionInfoRequest.setAmountInCents(new BigInteger("20000")); // Over the limit for tap and go  Limit is 100$
    transactionInfoRequest.setAmountInCents(new BigInteger("1000")); // Over the limit for tap and go  Limit is 100$
    //transactionInfoRequest.setAmountInCents(new BigInteger("905")); // will decline
    processPaymentRequest.setTransactionInfoRequest(transactionInfoRequest);
  
    // Set configuration parameters
    ProcessingParameters processingParameters = new ProcessingParameters();
 processingParameters.setResponseTimeoutInSeconds(BigInteger.valueOf(60));
       processingParameters.setPaymentProcessingMethod(PaymentProcessingMethod.PINPAD);
       processingParameters.setConfirmationRequired(Boolean.TRUE);
       processingParameters.setStoreCreditCardToVaultAndReturnToken(Boolean.FALSE);  // FALSE is no token required
       processingParameters.setCustomerMessageRequested(Boolean.TRUE);
       processingParameters.setAgentMessageRequested(Boolean.TRUE);
       processingParameters.setReceiptInfoRequested(Boolean.TRUE);
       processPaymentRequest.setProcessingParameters(processingParameters);
    // Process transaction
    processPaymentResponse = facade.processPayment(processPaymentRequest);
  
    // Get response and print
    ResponseStatus responseStatus;
    responseStatus = processPaymentResponse.getResponseStatus();
    // Response status
    System.out.println(" BCF Boolean Response Status: (True = SUCCESS) " + processPaymentResponse.getResponseStatus().isBcfPaymentResponseBooleanStatus()); // Internal Not printed on receipt
    System.out.println(" BCF Response code: " + processPaymentResponse.getResponseStatus().getBcfResponseCode());  // Internal Not printed on receipt
    System.out.println(" BCF response description: " + processPaymentResponse.getResponseStatus().getBcfResponseDescription());  // Internal Not printed on receipt
    System.out.println(" Service provider response code: " + processPaymentResponse.getResponseStatus().getServiceProviderResponseCode());
    System.out.println(" Service provider response description: " + processPaymentResponse.getResponseStatus().getServiceProviderResponseDescription());
    System.out.println(" ISO Response code: " + processPaymentResponse.getResponseStatus().getIsoResponseCode());
  
    // transaction info
    TransactionInfoResponse transactionInfoResponse;
    transactionInfoResponse = processPaymentResponse.getTransactionInfoResponse();
    if (transactionInfoResponse != null) {
            System.out.println(" Transaction Reference Number: " + transactionInfoResponse.getTransactionRefNumber());
            System.out.println(" Approval Number: " + transactionInfoResponse.getApprovalNumber());
            System.out.println(" Invoice Number: " + transactionInfoResponse.getInvoiceNumber());   // will be empty for PIN pad transactions
            System.out.println(" Transaction timestamp: " + transactionInfoResponse.getTransTimeStamp());
            System.out.println(" Transaction Type: " + transactionInfoResponse.getTransactionType());
            if (transactionInfoResponse.getCardEntryMethod() != null) {
                System.out.println(" Card Entry Method: " + transactionInfoResponse.getCardEntryMethod().value());
            }
            if (transactionInfoResponse.getCvmIndicator() != null) {
                System.out.println(" CVM Indicator: " + transactionInfoResponse.getCvmIndicator().value());
            }
            if (transactionInfoResponse.getSafIndicator() != null) {
                System.out.println(" SAF Indicator: " + transactionInfoResponse.getSafIndicator().value());
            }
  
            System.out.println(" Transaction amount in cents: " + transactionInfoResponse.getTransactionAmountInCents());
            System.out.println(" Terminal ID: " + transactionInfoResponse.getServiceProviderPaymentTerminalId());
            System.out.println(" BCF Reference Record: " + transactionInfoResponse.getBcfRefRecord());    // Internal Not printed on receipt
            System.out.println(" Transaction Session ID for confirm/reverse: " + transactionInfoResponse.getBcfTransactionSessionId());  // Internal Not printed on receipt or stored
      
            // EMV fields that may be empty
            System.out.println(" EMV TSI: " + transactionInfoResponse.getEmvTsi());
            System.out.println(" EMV TVR: " + transactionInfoResponse.getEmvTvr());
    }
  
    // Card info Response
    CardInfoResponse cardInfoResponse;
    cardInfoResponse = processPaymentResponse.getCardInfoResponse();
    if (cardInfoResponse != null) {
            System.out.println(" Card Type " + cardInfoResponse.getCardType());
                System.out.println(" Card name: " + cardInfoResponse.getCardNumberToPrint());  // Internal Not printed on receipt but kept in record
            System.out.println(" Debit account type: " + cardInfoResponse.getDebitAccountType());
            if (cardInfoResponse.getCardRef() != null) {
                System.out.println(" Card number to record " + cardInfoResponse.getCardRef().getCardValue()); //Not printed on receipt but kept in record
            }
            if (cardInfoResponse.getCardNumberToPrint() != null) {
                System.out.println(" Card number to print " + cardInfoResponse.getCardNumberToPrint().getCardValue());
            }
            System.out.println(" EMV application label: " + cardInfoResponse.getEmvApplicationLabel());
            System.out.println(" EMV Aid: " + cardInfoResponse.getEmvAid());
            System.out.println(" EMV application preferred name : " + cardInfoResponse.getEmvApplicationPreferredName());
            System.out.println(" CardHolderLanguage: " + cardInfoResponse.getCardHolderLanguage());
            if (cardInfoResponse.getTokenRef() != null) {
                System.out.println(" Token: " + cardInfoResponse.getTokenRef().getTokenValue());
            }
    }
  
    // receipt info Respomse
    ReceiptInfo receiptInfo;
    receiptInfo = processPaymentResponse.getReceiptInfo();
    if (receiptInfo != null) {
            if (receiptInfo.getTransactionType() != null) {
                System.out.println(" Transaction type " + receiptInfo.getTransactionType());
            }
            if (receiptInfo.getApprovalOrDeclineMessage() != null) {
            System.out.println(" Approval Or Decline Message " + receiptInfo.getApprovalOrDeclineMessage());
            }
            if (receiptInfo.getSupplementalMessage() != null) {
            System.out.println(" Supplemental Message " + receiptInfo.getSupplementalMessage());
            }
            if (receiptInfo.getEmvReversalMessage() != null) {
                System.out.println(" Emv Reversal Message " + receiptInfo.getEmvReversalMessage());
            }
            if (receiptInfo.getEmvPinEntryMessage() != null) {
                System.out.println(" Emv Pin Entry Message " + receiptInfo.getEmvPinEntryMessage());
            }
            if (receiptInfo.getEmvFallbackMessage() != null) {
                System.out.println(" Emv Fallback Message " + receiptInfo.getEmvFallbackMessage());
            }
            if (receiptInfo.getEmvChipCardMalfunctionMessage() != null) {
                System.out.println(" Emv Chip Card Malfunction Message " + receiptInfo.getEmvChipCardMalfunctionMessage());
            }
    } 
  
    // confirm
    ConfirmRequest confirmRequest = new ConfirmRequest();
    ConfirmResponse confirmResponse;
    confirmRequest.setSourceSystemInfo(sourceSystemInfo);
    if (transactionInfoResponse != null) {
        confirmRequest.setBcfTransactionSessionId(transactionInfoResponse.getBcfTransactionSessionId()); // get session ID from processed transaction
    }
  
    confirmRequest.setConfirmStatus(Boolean.TRUE); // true to confirm
    //confirmRequest.setConfirmStatus(Boolean.FALSE); // false to reverse     
    confirmResponse = facade.confirm(confirmRequest);  // send the confirm / reverse
    System.out.println(" BCF response code " + confirmResponse.getBcfResponseCode());     
}
}