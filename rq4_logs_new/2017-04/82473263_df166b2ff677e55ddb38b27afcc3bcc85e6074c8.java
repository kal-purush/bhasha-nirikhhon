package techpark.contoller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcPrint;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;
import techpark.user.UserProfile;


import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


/**
 * Created by Варя on 30.03.2017.
 */
@SuppressWarnings("unused")
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc(print = MockMvcPrint.NONE)
@Transactional
public class UserControllerTest {

    @Autowired
    protected MockMvc mockMvc;

    private final UserProfile user = new UserProfile("mail", "login", "password", null);


   @Before
    public void addUser() throws Exception{
        mockMvc.perform(post("/api/registration")
        .contentType(MediaType.APPLICATION_JSON_UTF8)
        .content("{\"mail\":\"" + user.getMail() + "\"," +
                        "\"login\":\"" + user.getLogin() + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
        .andExpect(status().isOk());
    }


    @Test
    public void testNotFound() throws Exception {
        mockMvc.perform(
                get("/hello"))
                .andExpect(status().isNotFound());
    }


    @Test
    public void testRegistration() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + '1' + "\"," +
                "\"login\":\"" + user.getLogin() + '1' + "\"," +
                "\"password\":\"" + user.getPassword() + '1' + "\"}"))
                .andExpect(status().isOk());
    }

    @Test
    public void testRegistrationEmtyParam() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isNotFound())
                .andExpect(content().json("{ \"error\" : \"Request param not found\"}"));
    }

    @Test
    public void testRegistrationEmtyParamValue() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\""  + "\"," +
                        "\"login\":\"" + user.getLogin() + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{ \"error\" : \"Wrong data\"}"));
    }

    @Test
    public void testRegistIncorrectParamValue() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + "" + "\"," +
                        "\"login\":\"" + "asa" + "\"," +
                        "\"password\":\"" + "123" + "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{ \"error\" : \"Wrong data\"}"));
    }

    @Test
    public void testRegistrationSameMail() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + '2' + "\"," +
                        "\"login\":\"" + user.getLogin() + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isConflict())
                .andExpect(content().json("{ \"error\" : \"Login already exist\"}"));
    }

    @Test
    public void testRegistrationSameLogin() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + "\"," +
                        "\"login\":\"" + user.getLogin() + '2' + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isConflict())
                .andExpect(content().json("{ \"error\" : \"E-mail already exist\"}"));
    }


    @Test
    public void testLogin() throws Exception {
        mockMvc.perform(post("/api/login")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"login\":\"" + user.getLogin() + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isOk());
    }

    @Test
    public void testLoginIncorrectLogin() throws Exception {
        mockMvc.perform(post("/api/login")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"login\":\"" + user.getLogin() + '3' + "\"," +
                        "\"password\":\"" + user.getPassword() + "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{ \"error\" : \"Wrong login or password\"}"));
    }

    @Test
    public void testLoginIncorrectPassword() throws Exception {
        mockMvc.perform(post("/api/login")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"login\": \"" + user.getLogin() + "\"," +
                        "\"password\":\"" + user.getPassword() + '3' +  "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{ \"error\" : \"Wrong login or password\"}"));
    }

    @Test
    public void testEditUser() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + '4' + "\"," +
                        "\"login\":\"" + user.getLogin() + '4' + "\"," +
                        "\"password\":\"" + user.getPassword() + '4' + "\"}"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/settings")
                .sessionAttr("Login", user.getLogin() + '4')
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + "newmail" + "\"," +
                        "\"login\":\"" + "newlogin" + "\"," +
                        "\"password\":\"" + "newpassword" + "\"}"))
                .andExpect(status().isOk());
    }

    @Test
    public void testEditUserOneValue() throws Exception {
        mockMvc.perform(post("/api/registration")
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"mail\":\"" + user.getMail() + '5' + "\"," +
                        "\"login\":\"" + user.getLogin() + '5' + "\"," +
                        "\"password\":\"" + user.getPassword() + '5' + "\"}"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/settings")
                .sessionAttr("Login", user.getLogin() + '5')
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content("{\"login\":\"" + "newlogin2" + "\"}" ))
                .andExpect(status().isOk());
    }

    @Test
    public void testBestUsers() throws Exception {
        mockMvc.perform(get("/api/users"))
                .andExpect(status().isOk());
    }
}