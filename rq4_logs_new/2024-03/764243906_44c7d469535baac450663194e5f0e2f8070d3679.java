package dev.enricosola.porcellino;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import dev.enricosola.porcellino.support.TestAuthenticationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.RequestBuilder;
import dev.enricosola.porcellino.support.DatabaseCleaner;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.junit.jupiter.api.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class PortfolioAPITest {
    @Autowired
    private TestAuthenticationManager testAuthenticationManager;

    @Autowired
    private DatabaseCleaner databaseCleaner;

    @Autowired
    private MockMvc mockMvc;

    private boolean initialized = false;

    @BeforeEach
    public void setup(){
        if ( !this.initialized ){
            this.databaseCleaner.clean();
            this.testAuthenticationManager.ensureTestUser();
            this.initialized = true;
        }
    }

    @Test
    @Order(1)
    @DisplayName("Testing invalid form data detection when creating a portfolio.")
    public void invalidSignupParameters() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = post("/api/portfolio/create")
                .header("Authorization", "Bearer " + authenticationToken);
        this.mockMvc.perform(requestBuilder).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERR_INVALID_FORM"))
                .andExpect(jsonPath("$.errors.currencyId[0]").isNotEmpty())
                .andExpect(jsonPath("$.errors.name[0]").isNotEmpty());
    }

    @Test
    @Order(2)
    @DisplayName("Testing new portfolio creation.")
    public void portfolioCreation() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = post("/api/portfolio/create")
                .header("Authorization", "Bearer " + authenticationToken)
                .param("currencyId", "1")
                .param("name", "Test");
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.portfolio.id").value(1))
                .andExpect(jsonPath("$.portfolio.name").value("Test"))
                .andExpect(jsonPath("$.portfolio.currency.id").value(1))
                .andExpect(jsonPath("$.portfolio.currency.name").isNotEmpty());
    }

    @Test
    @Order(3)
    @DisplayName("Testing portfolio listing.")
    public void portfolioListing() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = get("/api/portfolio")
                .header("Authorization", "Bearer " + authenticationToken);
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.portfolioList[0].id").value(1))
                .andExpect(jsonPath("$.portfolioList[0].name").value("Test"))
                .andExpect(jsonPath("$.portfolioList[0].currency.id").value(1))
                .andExpect(jsonPath("$.portfolioList[0].currency.name").isNotEmpty());
    }

    @Test
    @Order(4)
    @DisplayName("Testing portfolio editing using an invalid name.")
    public void invalidPortfolioEdit() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = patch("/api/portfolio/1/edit")
                .header("Authorization", "Bearer " + authenticationToken);
        this.mockMvc.perform(requestBuilder).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERR_INVALID_FORM"))
                .andExpect(jsonPath("$.errors.name[0]").isNotEmpty());
    }

    @Test
    @Order(5)
    @DisplayName("Testing previously created portfolio edit.")
    public void portfolioEdit() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = patch("/api/portfolio/1/edit")
                .header("Authorization", "Bearer " + authenticationToken)
                .param("name", "Test edited");
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.portfolio.id").value(1))
                .andExpect(jsonPath("$.portfolio.name").value("Test edited"))
                .andExpect(jsonPath("$.portfolio.currency.id").value(1))
                .andExpect(jsonPath("$.portfolio.currency.name").isNotEmpty());
    }

    @Test
    @Order(6)
    @DisplayName("Testing portfolio delete.")
    public void portfolioDelete() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = delete("/api/portfolio/1/delete")
                .header("Authorization", "Bearer " + authenticationToken);
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));
    }

    @Test
    @Order(7)
    @DisplayName("Testing portfolio listing when there is no portfolio left.")
    public void portfolioEmptyListing() throws Exception {
        String authenticationToken = this.testAuthenticationManager.getAuthenticationToken();
        RequestBuilder requestBuilder = get("/api/portfolio")
                .header("Authorization", "Bearer " + authenticationToken);
        this.mockMvc.perform(requestBuilder).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.portfolioList").isEmpty());
    }
}