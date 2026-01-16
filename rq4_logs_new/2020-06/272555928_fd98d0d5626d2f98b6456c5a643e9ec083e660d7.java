package br.com.orion.bank;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasEntry;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import br.com.orion.bank.model.dto.PaymentDto;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class PaymentControllerTest {

    @Test
    public void validate_payment_with_success_test()  {
        PaymentDto json = new PaymentDto();
        json.setCardNumber(12345678);
        json.setSecurityCode(90);
        json.setBillAmount(new BigDecimal(10));

        given()
            .contentType("application/json")
            .body(json)
            .post("/payment")
        .then()
            .assertThat()   
            .statusCode(201)
            .body("message", is("Payment has done.") )
            .log()
            .all();
    }

    @Test
    public void validate_null_cardNumber_test()  {
        PaymentDto json = new PaymentDto();
        json.setCardNumber(null);
        json.setSecurityCode(90);
        json.setBillAmount(new BigDecimal(10));

        given()
            .contentType("application/json")
            .body(json)
            .post("/payment")
        .then()
            .assertThat()   
            .statusCode(400)
            .body("message", is("Invalid arguments") )
            .body("errors.size()", is(1))
            .body("errors", hasEntry("cardNumber", "Card number can't be null.") )
            .log()
            .all();
    }

    @Test
    public void validate_balance_insuficient_test()  {
        PaymentDto json = new PaymentDto();
        json.setCardNumber(12345678);
        json.setSecurityCode(90);
        json.setBillAmount(new BigDecimal(5000));

        given()
            .contentType("application/json")
            .body(json)
            .post("/payment")        
        .then()
            .assertThat()   
            .statusCode(406)
            .body("message", is("There is no enough balance"))
            .log()
            .all();
    }

    @Test
    public void validate_invalid_cardNumber_test()  {
        PaymentDto json = new PaymentDto();
        json.setCardNumber(1234567);
        json.setSecurityCode(90);
        json.setBillAmount(new BigDecimal(10));

        given()
            .contentType("application/json")
            .body(json)
            .post("/payment")
        .then()
            .assertThat()   
            .statusCode(406)
            .body("message", is("Invalid card number."))
            .log()
            .all();
    }

}