package org.ua.javaPro.berezhnoy.bank.hillelHomwWork28DBVersioning;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@SpringBootTest
@TestPropertySource("classpath:application-test.properties")
class PersonAndAccountControllerIntegrationTest {
    @Autowired
    protected PersonRepository personRepository;
    @Autowired
    protected AccountRepository accountRepository;
    @Autowired
    protected MockMvc mockMvc;

    @Test
    void shouldCreateAccount() throws Exception {
        var uuidPerson = UUID.randomUUID().toString();
        personRepository.save(Person.builder()
                .uid(uuidPerson)
                .name("testtt")
                .email("@testtt")
                .build());

        var createAccount = post("/api/persons/" + uuidPerson + "/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\n\"iban\":\"ua111111111\",\n\"balance\": \"11111111\" \n}");

        mockMvc.perform(createAccount)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.uid").isNotEmpty())
                .andReturn();

    }

    @Test
    void shouldGetAccount() throws Exception {
        var uuidPerson = UUID.randomUUID().toString();
        personRepository.save(Person.builder()
                .uid(uuidPerson)
                .name("testtt")
                .email("@testtt")
                .build());

        var createAccount = post("/api/persons/" + uuidPerson + "/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\n\"iban\":\"ua111111111\",\n\"balance\": \"100\" \n}");

        MvcResult result = mockMvc.perform(createAccount)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.uid").isNotEmpty())
                .andReturn();

        String response = result.getResponse().getContentAsString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response);
        String accountUuid = jsonNode.get("uid").asText();

        var getAccount = get("/api/persons/accounts/" + accountUuid);

        mockMvc.perform(getAccount)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.balance").value("100"));
    }


    @Test
    void shouldUpdateAccountBalance() throws Exception {
        var uuidPerson = UUID.randomUUID().toString();
        personRepository.save(Person.builder()
                .uid(uuidPerson)
                .name("testtt")
                .email("@testtt")
                .build());

        var createAccount = post("/api/persons/" + uuidPerson + "/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\n\"iban\":\"ua111111111\",\n\"balance\": \"100\" \n}");

        MvcResult result = mockMvc.perform(createAccount)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.uid").isNotEmpty())
                .andReturn();

        String response = result.getResponse().getContentAsString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response);
        String accountUuid = jsonNode.get("uid").asText();


        var updateAccount = put("/api/persons/accounts/" + accountUuid)
                .contentType(MediaType.APPLICATION_JSON)
                .content("200");

        mockMvc.perform(updateAccount)
                .andExpect(status().isOk());

        var getAccount = get("/api/persons/accounts/" + accountUuid);

        mockMvc.perform(getAccount)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.balance").value("200"));
    }

    @Test
    void shouldDeleteAccount() throws Exception {
        var uuidPerson = UUID.randomUUID().toString();
        personRepository.save(Person.builder()
                .uid(uuidPerson)
                .name("testtt")
                .email("@testtt")
                .build());

        var createAccount = post("/api/persons/" + uuidPerson + "/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\n\"iban\":\"ua111111111\",\n\"balance\": \"100\" \n}");

        MvcResult result = mockMvc.perform(createAccount)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.iban").value("ua111111111"))
                .andExpect(jsonPath("$.uid").isNotEmpty())
                .andReturn();

        String response = result.getResponse().getContentAsString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response);
        String accountUuid = jsonNode.get("uid").asText();

        var deleteAccount = delete("/api/persons/" + accountUuid + "/accounts");
        mockMvc.perform(deleteAccount)
                .andExpect(status().isOk());

    }

}