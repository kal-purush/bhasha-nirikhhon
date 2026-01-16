package com.example.demo.controllertests;

import com.example.demo.model.requests.CreateUserRequest;
import com.example.demo.model.requests.ModifyCartRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJsonTesters;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.net.URI;

import static org.junit.Assert.*;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureJsonTesters
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@ActiveProfiles("test")
public class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JacksonTester<ModifyCartRequest> json;

    @Autowired
    private JacksonTester<CreateUserRequest> jsonCreateUser;

    private String token = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcmFiIn0.fWLuNUE3ELAm0S5h4vqHufcvIgQyDNXfycH9hMXeJjXLRR3G9aAF18gFcKn5frSg-XAfPa1Mzffc9iz1B0bn8w";

    @Test
    public void shouldBeAbleToSubmitOrder() throws Exception {
        createUser("prab");
        ModifyCartRequest modifyCartRequest = new ModifyCartRequest();
        modifyCartRequest.setUsername("prab");
        modifyCartRequest.setItemId(1);
        modifyCartRequest.setQuantity(1);
        mockMvc.perform(MockMvcRequestBuilders.post(new URI("/api/cart/addToCart"))
                .content(json.write(modifyCartRequest).getJson())
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", token)
        )
                .andExpect(status().isOk());

        mockMvc.perform(MockMvcRequestBuilders.post(new URI("/api/order/submit/prab"))
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", token))
                .andExpect(status().isOk());
    }

    @Test
    public void shouldBeAbleToSeeSubmittedOrders() throws Exception{
        createUser("test");
        ModifyCartRequest modifyCartRequest = new ModifyCartRequest();
        modifyCartRequest.setUsername("test");
        modifyCartRequest.setItemId(1);
        modifyCartRequest.setQuantity(1);
        mockMvc.perform(MockMvcRequestBuilders.post(new URI("/api/cart/addToCart"))
                .content(json.write(modifyCartRequest).getJson())
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", token))
                .andExpect(status().isOk());

        mockMvc.perform(MockMvcRequestBuilders.post(new URI("/api/order/submit/test"))
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", token))
                .andExpect(status().isOk());

       MvcResult orders = mockMvc.perform(MockMvcRequestBuilders.get(new URI("/api/order/history/test"))
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", token))
                .andExpect(status().isOk())
                .andReturn();
        Assert.assertNotNull(orders.getResponse().getContentAsString());
    }

    private void createUser(String username) throws Exception{
        CreateUserRequest createUserRequest = new CreateUserRequest();
        createUserRequest.setUsername(username);
        createUserRequest.setPassword("test_password");
        createUserRequest.setConfirmPassword("test_password");
        mockMvc.perform(MockMvcRequestBuilders.post(new URI("/api/user/create"))
                .content(jsonCreateUser.write(createUserRequest).getJson())
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }
}