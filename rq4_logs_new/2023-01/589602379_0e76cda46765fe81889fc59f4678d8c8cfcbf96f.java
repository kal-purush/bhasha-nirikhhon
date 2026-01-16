package de.neuefische.neuefischespringsecuritydemo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class CarControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Test
    void getAll_whenNotLoggedIn_shouldReturn401 () throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/cars"))
            .andExpectAll(
            MockMvcResultMatchers.status().isUnauthorized()
            );
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    void getAll_whenLoggedInAsUser_shouldReturnCars () throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/cars"))
            .andExpectAll(
                MockMvcResultMatchers.status().isOk(),
                MockMvcResultMatchers.content().json(
                    "[]",
                    true
                )
            );
    }
}