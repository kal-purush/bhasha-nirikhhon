package jm.stockx.controller.rest;

import jm.stockx.UserServiceImpl;
import jm.stockx.dto.UserDTO;
import jm.stockx.entity.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebAppConfiguration
@RunWith(SpringRunner.class)
public class RegistrationRestControllerTest {
    private static final String url = "/registration";

    @InjectMocks
    private RegistrationRestController regRestController;

    private MockMvc mockMvc;

    @Mock
    private UserServiceImpl userService;

    @Before
    public void init(){
        mockMvc = MockMvcBuilders.standaloneSetup(regRestController).build();
    }

    @Test
    public void registrationTest() throws Exception {
        UserDTO userDTO = new UserDTO();
        userDTO.setFirstName("John");
        userDTO.setLastName("Snow");
        userDTO.setEmail("winterfell@mail.ru");
        userDTO.setPassword("winter_is_coming");

        User user = new User(userDTO);
        mockMvc.perform(post(url)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(TestUtils.objectToJson(userDTO)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.firstName").value(user.getFirstName()))
                .andExpect(jsonPath("$.lastName").value(user.getLastName()))
                .andExpect(jsonPath("$.email").value(user.getEmail()))
                .andExpect(jsonPath("$.password").value(user.getPassword()));
    }
}