package gunes.cashcard.controller;


import gunes.cashcard.domain.Transaction;
import gunes.cashcard.ondemand.CashCardTransactionOnDemand;
import gunes.cashcard.stream.CashCardTransactionStream;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CashCardController {

    private CashCardTransactionOnDemand cashCardTransactionOnDemand;

    public CashCardController(CashCardTransactionOnDemand cashCardTransactionOnDemand) {
        this.cashCardTransactionOnDemand = cashCardTransactionOnDemand;
    }

    @PostMapping(path = "/publish/txn")
    public void publishTxn(@RequestBody Transaction transaction) {
        this.cashCardTransactionOnDemand.publishOnDemand(transaction);
    }
}