package com.abneco.delivery.acceptance.steps;


import com.abneco.delivery.address.service.AddressService;
import com.abneco.delivery.exception.RequestException;
import com.abneco.delivery.fee.controller.FeeController;
import com.abneco.delivery.fee.dto.CepForm;
import com.abneco.delivery.fee.dto.FeeResponse;
import com.abneco.delivery.fee.service.FeeService;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.jupiter.api.Assertions;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeeStep {

    private FeeController controller;
    private CepForm form;
    private CepForm invalidForm;
    private CepForm formNullCep;

    public static final String CEP = "04851280";
    public static final String INVALID_CEP = "A1234567";

    @Before
    public void setup() {
        RestTemplate restTemplate = new RestTemplate();
        AddressService addressService = new AddressService(restTemplate);
        FeeService service = new FeeService(addressService);
        this.controller = new FeeController(service);
        this.form = new CepForm(CEP);
        this.invalidForm = new CepForm(INVALID_CEP);
        this.formNullCep = new CepForm(null);
    }

    //valid cep scenario
    @Given("valid cep")
    public void valid_cep() {
        new CepForm(CEP);
    }

    @When("request is made")
    public void request_is_made() {
        controller.getDeliveryFeeByCep(form);
    }

    @Then("FeeResponse is returned according to cep")
    public void feeResponse_is_returned_according_to_cep() {
        FeeResponse response = controller.getDeliveryFeeByCep(form);
        assertNotNull(response);
        assertEquals(new BigDecimal("7.85"), response.getFrete());
        assertEquals("SP", response.getEstado());
    }

    //invalid cep scenario
    @Given("invalid cep")
    public void invalid_cep() {
        new CepForm(CEP);
    }

    @When("request is made with invalid cep")
    public void request_is_made_with_invalid_cep() {
        assertThrows(RequestException.class, () -> controller.getDeliveryFeeByCep(invalidForm));
    }

    @Then("RequestException is thrown explaining the error")
    public void requestException_is_thrown_explaining_the_error() {
        Exception exception = assertThrows(RequestException.class, () -> controller.getDeliveryFeeByCep(invalidForm));
        Assertions.assertNotNull(exception);
        assertEquals("Please verify if cep has 8 numbers, and numbers only.", exception.getMessage());
    }

    //null cep scenario
    @Given("null cep")
    public void null_cep() {
        new CepForm(CEP);
    }

    @When("request is made with null cep")
    public void request_is_made_with_null_cep() {
        assertThrows(RequestException.class, () -> controller.getDeliveryFeeByCep(formNullCep));
    }

    @Then("RequestException is thrown explaining that field cep is mandatory")
    public void requestException_is_thrown_explaining_that_field_cep_is_mandatory() {
        Exception exception = assertThrows(RequestException.class, () -> controller.getDeliveryFeeByCep(formNullCep));
        Assertions.assertNotNull(exception);
        assertEquals("Cep is mandatory.", exception.getMessage());
    }

}