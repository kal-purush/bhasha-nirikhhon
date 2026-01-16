package hr.tvz.keepthechange;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import hr.tvz.keepthechange.dto.UserRegistrationDto;
import hr.tvz.keepthechange.service.UserService;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Login Test class.
 */
@SpringBootTest
@AutoConfigureMockMvc
public class RegistrationTest {

    @Autowired
    private MockMvc mockMvc;

    /**
     * Test for performing login. Accessing index page without login will fail.
     * @throws Exception exception
     */
    @Test
    public void loginTest() throws Exception {
        this.mockMvc
                .perform(get("/registration").with(user("a").password("a").password("a").password("a")
                        .roles("USER")))
                .andExpect(status().isOk())
                .andExpect(view().name("registration"));
    }
}