package dev.amir.usercommand.it;

import dev.amir.usercommand.application.port.output.UserMessageOutputPort;
import dev.amir.usercommand.application.port.output.UserOutputPort;
import dev.amir.usercommand.domain.entity.User;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class UserCommandServiceIT {
    @MockBean
    UserOutputPort userOutputPort;
    @MockBean
    UserMessageOutputPort userMessageOutputPort;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void createUserTest() throws Exception {
        User mockUser = new User();
        String expectedUUID = UUID.randomUUID().toString();
        mockUser.setId(expectedUUID);
        when(userOutputPort.save(any(User.class))).thenReturn(mockUser);
        doNothing().when(userMessageOutputPort).sendMessage(any());

        String body = new String(getClass().getResourceAsStream("/responses/create-users-response.json").readAllBytes(), StandardCharsets.UTF_8);

        MvcResult response = mockMvc.perform(MockMvcRequestBuilders
                        .post("/user")
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();

        String newUserId = response.getResponse().getContentAsString();
        assertEquals(expectedUUID, newUserId);

        verify(userOutputPort).save(any(User.class));
        verify(userMessageOutputPort).sendMessage(any(User.class));
    }

    @Test
    public void deleteUserTest() throws Exception {
        String expectedUUID = UUID.randomUUID().toString();
        when(userOutputPort.delete(any(String.class))).thenReturn(true);

        MvcResult response = mockMvc.perform(MockMvcRequestBuilders
                        .delete("/user/{id}", expectedUUID))
                .andExpect(status().isOk())
                .andReturn();

        String result = response.getResponse().getContentAsString();
        assertEquals("true", result);

        verify(userOutputPort).delete(eq(expectedUUID));
    }

    @Test
    void updateUserTest() throws Exception {
        User mockUser = new User();
        String expectedUUID = UUID.randomUUID().toString();
        mockUser.setId(expectedUUID);
        when(userOutputPort.save(any(User.class))).thenReturn(mockUser);
        doNothing().when(userMessageOutputPort).sendMessage(any());

        String body = new String(getClass().getResourceAsStream("/responses/update-users-response.json").readAllBytes(), StandardCharsets.UTF_8);

        mockMvc.perform(MockMvcRequestBuilders
                        .put("/user/{id}", expectedUUID)
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        verify(userOutputPort).save(any(User.class));
        verify(userMessageOutputPort).sendMessage(any(User.class));
    }
}