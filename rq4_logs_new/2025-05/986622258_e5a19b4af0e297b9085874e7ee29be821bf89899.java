package gunes.cashcard.controller;

import gunes.cashcard.domain.CashCard;
import gunes.cashcard.domain.Transaction;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CashCardControllerTests {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void postShouldSendTransactionAsAMessage() throws IOException {
        Transaction postedTransaction = new Transaction(123L, new CashCard(1L, "kumar2", 1.00));
        ResponseEntity<Transaction> response = this.restTemplate.postForEntity(
                "http://localhost:" + port + "/publish/txn",
                postedTransaction, Transaction.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}