package com.abneco.delivery.acceptance.steps;

import com.abneco.delivery.exception.RequestException;
import com.abneco.delivery.user.controller.SellerController;
import com.abneco.delivery.user.json.SellerForm;
import com.abneco.delivery.user.mock.MockSellerRepository;
import com.abneco.delivery.user.repository.SellerRepository;
import com.abneco.delivery.user.service.SellerService;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SellerStep {

    private SellerController controller;
    private SellerForm form;
    private SellerForm invalidEmailForm;
    private List<String> invalidCnpjs;
    private Exception exception;

    @Before
    public void setup() {
        SellerRepository repository = new MockSellerRepository();
        SellerService service = new SellerService(repository);
        this.controller = new SellerController(service);
    }

    @Given("the following seller data:")
    public void seller_form_valid_fields(DataTable dataTable) {
        List<Map<String, String>> users = dataTable.asMaps();

        for (Map<String, String> user : users) {
            String name = user.get("name");
            String email = user.get("email");
            String password = user.get("password");
            Long phoneNumber = Long.valueOf(user.get("phoneNumber"));
            String cnpj = user.get("cnpj");
            this.form = new SellerForm(name, email, password, phoneNumber, cnpj);
        }
    }

    @When("a request to register the seller is made")
    public void request_register_sellers_success() {
        controller.registerSeller(form);
    }

    @Then("the seller should be successfully registered")
    public void sellers_are_registered() {
        Assertions.assertDoesNotThrow(() -> controller.registerSeller(form));
    }


    @Given("the following seller data with wrong email:")
    public void invalid_email_field_seller_form(DataTable dataTable) {
        List<Map<String, String>> users = dataTable.asMaps();

        for (Map<String, String> user : users) {
            String name = user.get("name");
            String email = user.get("email");
            String password = user.get("password");
            Long phoneNumber = Long.valueOf(user.get("phoneNumber"));
            String cnpj = user.get("cnpj");
            this.invalidEmailForm = new SellerForm(name, email, password, phoneNumber, cnpj);
        }
    }

    @When("a request to register the seller with incorrect email is made")
    public void request_register_seller_with_incorrect_email() {
        assertThrows(RequestException.class, () -> controller.registerSeller(invalidEmailForm));
    }

    @Then("exception thrown explaining that email has incorrect format")
    public void incorrect_email_exception() {
        Exception exception = assertThrows(RequestException.class, () -> controller.registerSeller(invalidEmailForm));
        assertNotNull(exception);
        assertEquals("Email has incorrect format.", exception.getMessage());
    }

    @Given("seller with incorrect cnpjs")
    public void invalid_cnpj_field_seller_form(List<String> cnpjs) {
        this.invalidCnpjs = cnpjs;
    }

    @When("a request to register the seller with incorrect cnpj is made")
    public void request_register_seller_with_incorrect_cnpj() {
        for (String cnpj : invalidCnpjs) {
            SellerForm invalidCnpjForm = new SellerForm("John Doe", "john.doe@example.com", "password", 1234567890L, cnpj);
            try {
                controller.registerSeller(invalidCnpjForm);
            } catch (RequestException e) {
                this.exception = e;
            }
        }
    }

    @Then("exception thrown explaining that cnpj should have only 14 chars and only numbers")
    public void incorrect_cnpj_exception() {
        assertEquals("Cnpj must have 14 numbers, and numbers only.", exception.getMessage());
    }
}