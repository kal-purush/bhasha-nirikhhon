package com.abneco.delivery.address.controller;

import com.abneco.delivery.address.dto.AddressForm;
import com.abneco.delivery.address.entity.Address;
import com.abneco.delivery.address.repository.AddressRepository;
import com.abneco.delivery.address.service.AddressService;
import com.abneco.delivery.user.entity.Seller;
import com.abneco.delivery.user.json.SellerForm;
import com.abneco.delivery.user.repository.SellerRepository;
import com.abneco.delivery.user.service.SellerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class AddressControllerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AddressService service;
    @Autowired
    private SellerService sellerService;

    @Autowired
    private AddressRepository repository;

    @Autowired
    private SellerRepository sellerRepository;
    public static final String EMAIL = "email2.string@email.com";
    public static final String CNPJ = "12348765324123";
    public static final String NAME = "seller1";
    public static final String PASSWORD = "12345678";
    public static final Long PHONE_NUMBER = 11987654321L;
    public static final String CEP = "04555000";
    public static final String COMPLEMENTO = "";
    public static final Integer NUMERO = 123;

    @Test
    void test_get_all_addresses() throws Exception {
        SellerForm sellerForm = new SellerForm(NAME, EMAIL, PASSWORD, PHONE_NUMBER, CNPJ);
        sellerService.registerSeller(sellerForm);
        Seller seller = sellerRepository.findByEmail(sellerForm.getEmail()).get();

        AddressForm addressForm = new AddressForm(seller.getId(), CEP, COMPLEMENTO, NUMERO);
        service.registerAddressByCep(addressForm);
        Address address = repository.findBySellerId(seller.getId()).get();

        mockMvc.perform(get("/address")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        repository.deleteById(address.getId());
        sellerRepository.deleteById(seller.getId());
    }

    @Test
    void test_get_all_addresses_no_content() throws Exception {
        mockMvc.perform(get("/address")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent());
    }
}