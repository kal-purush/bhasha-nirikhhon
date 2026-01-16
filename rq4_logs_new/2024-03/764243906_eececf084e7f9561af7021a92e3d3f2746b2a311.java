package dev.enricosola.porcellino;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.junit.jupiter.api.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class UserAPITest {
    private boolean initialized = false;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MockMvc mockMvc;

    @BeforeEach
    public void setup(){
        if ( !this.initialized ){System.out.println("CLEAN");
            this.jdbcTemplate.execute("TRUNCATE TABLE users;");
            this.initialized = true;
        }
    }

    @Order(1)
    @Test
    @DisplayName("Testing invalid form data detection when signing up an user.")
    public void invalidSignupParameters() throws Exception {
        this.mockMvc.perform(post("/api/user/signup")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERR_INVALID_FORM"))
                .andExpect(jsonPath("$.errors.password[0]").isNotEmpty())
                .andExpect(jsonPath("$.errors.email[0]").isNotEmpty());
    }

    @Order(2)
    @Test
    @DisplayName("Testing user account creation.")
    public void userSignup() throws Exception {
        RequestBuilder requestBuilder = post("/api/user/signup")
                .param("password", "test_password")
                .param("email", "test@test.it");
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.user.email").value("test@test.it"))
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.token").isNotEmpty());
    }

    @Order(3)
    @Test
    @DisplayName("Testing duplicated email address detection.")
    public void duplicatedEmailAddress() throws Exception {
        RequestBuilder requestBuilder = post("/api/user/signup")
                .param("password", "test_password")
                .param("email", "test@test.it");
        this.mockMvc.perform(requestBuilder).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERR_EMAIL_ADDRESS_TAKEN"));
    }

    @Order(4)
    @Test
    @DisplayName("Testing invalid form data detection when authenticating an user.")
    public void invalidLoginParameters() throws Exception {
        this.mockMvc.perform(post("/api/auth/login")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERR_INVALID_FORM"))
                .andExpect(jsonPath("$.errors.password[0]").isNotEmpty())
                .andExpect(jsonPath("$.errors.email[0]").isNotEmpty());
    }

    @Order(5)
    @Test
    @DisplayName("Testing user authentication.")
    public void userLogin() throws Exception {
        RequestBuilder requestBuilder = post("/api/auth/login")
                .param("password", "test_password")
                .param("email", "test@test.it");
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.user.email").value("test@test.it"))
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.token").isNotEmpty());
    }

    @Order(6)
    @Test
    @DisplayName("Testing user not found detection.")
    public void userNotFound() throws Exception {
        RequestBuilder requestBuilder = post("/api/auth/login")
                .param("password", "test_password")
                .param("email", "not-found@test.it");
        this.mockMvc.perform(requestBuilder).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value("ERR_NOT_FOUND"));
    }

    @Order(7)
    @Test
    @DisplayName("Testing user invalid credentials detection.")
    public void invalidCredentials() throws Exception {
        RequestBuilder requestBuilder = post("/api/auth/login")
                .param("password", "invalid_password")
                .param("email", "test@test.it");
        this.mockMvc.perform(requestBuilder).andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value("ERR_UNAUTHORIZED"));
    }
}