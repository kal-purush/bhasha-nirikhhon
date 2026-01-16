package hr.tvz.keepthechange;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Login Test class.
 */
@SpringBootTest
@AutoConfigureMockMvc
public class LoginTest {

    @Autowired
    private MockMvc mockMvc;

    /**
     * Test for performing login. Accessing index page without login will fail.
     * @throws Exception exception
     */
    @Test
    public void loginTest() throws Exception {
        this.mockMvc
                .perform(get("/index").with(user("admin").password("adminpass")
                        .roles("USER", "ADMIN")))
                .andExpect(status().isOk())
                .andExpect(view().name("index"));
    }
}