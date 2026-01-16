package example.web.api;

import example.AbstractControllerTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class GreetingControllerITest extends AbstractControllerTest {

    private String stringHelper;

    @Before
    public void setUp() {
        super.mvc = MockMvcBuilders.webAppContextSetup(this.context).build();
    }

    @Test
    public void findAll() throws Exception {
        mvc.perform(get("/api/greetings")).andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].id", is(1)))
                .andExpect(jsonPath("$.[0].text", is("Hello World")))
                .andExpect(jsonPath("$.[1].id", is(2)))
                .andExpect(jsonPath("$.[1].text", is("Hola Mundo")));
    }

    @Test
    public void findOne() throws Exception {
        mvc.perform(get("/api/greetings/1")).andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(1)))
                .andExpect(jsonPath("$.text", is("Hello World")));
    }

    @Test
    public void create() throws Exception {
        stringHelper = "{\"text\":\"Bonjour le monde!\"}";

        ResultActions result = mvc.perform(post("/api/greetings").header("Content-Type", "application/json").content(stringHelper)).andExpect(status().isCreated());

        MockHttpServletResponse content = result.andReturn().getResponse();
        logger.info("\nContent: " + content.getContentAsString());

        result.andExpect(jsonPath("$.id", is(3))).andExpect(jsonPath("$.text", is("Bonjour le monde!")));
    }

    @Test
    public void update() throws Exception {
        stringHelper = "{\n" +
                "  \"id\": \"2\",\n" +
                "  \"text\": \"Mudando Texto !\"\n" +
                "}";

        mvc.perform(put("/api/greetings/2").header("Content-Type", "application/json").content(stringHelper)).andExpect(status().isOk())

                .andExpect(jsonPath("$.id", is(2)))
                .andExpect(jsonPath("$.text", is("Mudando Texto !")));
    }

    @Test
    public void deleteTest() throws Exception {
        mvc.perform(delete("/api/greetings/2")).andExpect(status().isNoContent());
    }
}