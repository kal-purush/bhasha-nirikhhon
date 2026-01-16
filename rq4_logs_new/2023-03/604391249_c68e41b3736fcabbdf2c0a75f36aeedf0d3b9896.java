package com.abneco.delivery.user.controller;

import com.abneco.delivery.user.entity.Seller;
import com.abneco.delivery.user.json.SellerForm;
import com.abneco.delivery.user.repository.SellerRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SellerControllerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SellerRepository repository;

    public static final String ID = "lkajsçdlgnçblkdrt98709lsdkjfn,manfg";
    public static final String EMAIL = "string2341@email.com";
    public static final String EMAIL2 = "19823@email.com";
    public static final String CNPJ = "12348765324123";
    public static final String NAME = "seller1";
    public static final String PASSWORD = "12345678";
    public static final Long PHONE_NUMBER = 11987654321L;
    public static final Boolean EMAIL_VERIFIED = false;
    public static final String CREATED_AT = "";
    public static final String UPDATED_AT = null;

    @Test
    void test_register_seller() throws Exception {
        SellerForm form = new SellerForm(NAME, EMAIL, PASSWORD, PHONE_NUMBER, CNPJ);
        mockMvc.perform(post("/seller")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(form)))
                .andExpect(status().isCreated());
    }

    @Test
    void test_register_seller_email_already_in_use() throws Exception {
        SellerForm form = new SellerForm(NAME, EMAIL2, PASSWORD, PHONE_NUMBER, CNPJ);
        repository.save(new Seller(ID, form.getEmail(), CNPJ, NAME, PASSWORD, PHONE_NUMBER, EMAIL_VERIFIED, CREATED_AT, UPDATED_AT));
        mockMvc.perform(post("/seller")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(form)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Email already in use."));
    }

    @Test
    void test_register_seller_email_not_formated() throws Exception {

        SellerForm form = new SellerForm(NAME, "email", PASSWORD, PHONE_NUMBER, CNPJ);
        mockMvc.perform(post("/seller")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(form)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Email has incorrect format."));
    }
}