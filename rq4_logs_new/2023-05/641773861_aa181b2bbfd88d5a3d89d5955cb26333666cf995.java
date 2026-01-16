package fiftyfive.administration.usermanagement.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import fiftyfive.administration.usermanagement.constant.Constant;
import fiftyfive.administration.usermanagement.dto.CreateUserRequest;
import fiftyfive.administration.usermanagement.dto.UpdateUserRequestData;
import fiftyfive.administration.usermanagement.dto.UserResponseData;
import fiftyfive.administration.usermanagement.entity.User;
import fiftyfive.administration.usermanagement.exception.RecordAlreadyExistsException;
import fiftyfive.administration.usermanagement.exception.UserControllerAdvice;
import fiftyfive.administration.usermanagement.exception.UserNotExistsException;
import fiftyfive.administration.usermanagement.implemention.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.modelmapper.ModelMapper;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@ExtendWith(SpringExtension.class)
class UserManagementControllerTest {
    final StringBuilder userUri = new StringBuilder("/v1/users");

    ObjectMapper objectMapper = new ObjectMapper();
    @InjectMocks
    private UserManagementController userManagementController;
    @Mock
    private UserService userService;
    private MockMvc mockMvc;
    private User existingUser = new User();
    private CreateUserRequest createUserRequest = new CreateUserRequest();

    private UserResponseData userResponseData = new UserResponseData();

    private UpdateUserRequestData updateUserRequestData = new UpdateUserRequestData();

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        ModelMapper modelMapper = new ModelMapper();
        mockMvc = MockMvcBuilders.standaloneSetup(userManagementController)
                .setControllerAdvice(new UserControllerAdvice())
                .build();

        existingUser.setId(1L);
        existingUser.setFirstName("Jane");
        existingUser.setUsername("janedane");
        existingUser.setLastName("Smith");
        existingUser.setRole("user");
        existingUser.setPassword("password");
        existingUser.setCreatedAt(LocalDateTime.now());
        existingUser.setUpdatedAt(existingUser.getCreatedAt());
        createUserRequest = modelMapper.map(existingUser, CreateUserRequest.class);
        updateUserRequestData = modelMapper.map(existingUser, UpdateUserRequestData.class);
        userResponseData = modelMapper.map(existingUser, UserResponseData.class);
    }

    @Test
    void testCreateUser_WhenPasswordIsNull_ReturnsBadRequest() throws Exception {
        createUserRequest.setPassword(null);
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Password is required"));
    }

    @Test
    void testCreateUser_WhenRoleIsNull_ReturnsBadRequest() throws Exception {
        createUserRequest.setRole(null);
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Role is required"));
    }

    @Test
    void testCreateUser_WhenUsernameIsNull_ReturnsBadRequest() throws Exception {
        createUserRequest.setUsername(null);
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Username is required"));
    }

    @Test
    void testCreateUser_WhenFirstNameIsNull_ReturnsBadRequest() throws Exception {
        createUserRequest.setFirstName(null);
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("First Name is required"));
    }

    @Test
    void testCreateUser_WhenLastNameIsNull_InvalidRequest_ReturnsBadRequest() throws Exception {
        createUserRequest.setLastName(null);
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Last Name is required"));
    }

    @Test
    void testCreateUser_WhenRecordAlreadyExistsException_ReturnConflict() throws Exception {
        Mockito.doThrow(new RecordAlreadyExistsException(String.format(Constant.USER_ALREADY_EXISTS, 1L)))
                .when(userService)
                .createUser(Mockito.any(CreateUserRequest.class));

        String requestJson = objectMapper.writeValueAsString(createUserRequest);

        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(String.format(Constant.USER_ALREADY_EXISTS, 1L)));
    }

    @Test
    void testCreateUser_Successfully_ReturnsCreated() throws Exception {
        String requestJson = objectMapper.writeValueAsString(createUserRequest);
        when(userService.createUser(createUserRequest)).thenReturn(userResponseData);
        mockMvc.perform(post(userUri.toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.username").value(userResponseData.getUsername()))
                .andExpect(jsonPath("$.first_name").value(userResponseData.getFirstName()))
                .andExpect(jsonPath("$.last_name").value(userResponseData.getLastName()))
                .andExpect(jsonPath("$.role").value(userResponseData.getRole()))
                .andExpect(jsonPath("$.id").value(userResponseData.getId()))
                .andExpect(jsonPath("$.created_at").exists())
                .andExpect(jsonPath("$.updated_at").exists());

    }

    @Test
    void testUpdateUser_WhenUserNotExistsException_ReturnNotFound() throws Exception {
        Mockito.doThrow(new UserNotExistsException(String.format(Constant.USER_NOT_EXISTS, 1L)))
                .when(userService)
                .updateUser(Mockito.any(UpdateUserRequestData.class), any());
        String requestJson = objectMapper.writeValueAsString(updateUserRequestData);
        mockMvc.perform(put(userUri.append("/{userId}").toString(), 1L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value(String.format(Constant.USER_NOT_EXISTS, 1L)));
    }

    @Test
    void testUpdateUser_Successfully_ReturnsOk() throws Exception {
        when(userService.updateUser(updateUserRequestData, 1L)).thenReturn(userResponseData);
        String requestJson = objectMapper.writeValueAsString(updateUserRequestData);
        mockMvc.perform(put(userUri.append("/{userId}").toString(), 1L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isOk()).andExpect(jsonPath("$.first_name").value(userResponseData.getFirstName()))
                .andExpect(jsonPath("$.last_name").value(userResponseData.getLastName()))
                .andExpect(jsonPath("$.role").value(userResponseData.getRole()))
                .andExpect(jsonPath("$.id").value(userResponseData.getId()))
                .andExpect(jsonPath("$.created_at").exists())
                .andExpect(jsonPath("$.updated_at").exists());
    }

    @Test
    void testDeleteUser_Successfully_ReturnsNoContent() throws Exception {
        mockMvc.perform(delete(userUri.append("/{userId}").toString(), 1L)).andExpect(status().isNoContent());

    }

    @Test
    void testGetAllUser_Successfully_ReturnsOk() throws Exception {
        List<UserResponseData> userList = new ArrayList<>();
        userList.add(userResponseData);
        when(userService.getAllUsers()).thenReturn(userList);
        mockMvc.perform(get(userUri.toString()))
                .andExpect(status().isOk());
    }

    @Test
    void testGetAllUser_WhenNoUserExists_ReturnsNoContent() throws Exception {
        List<UserResponseData> userList = Collections.emptyList();
        when(userService.getAllUsers()).thenReturn(userList);

        mockMvc.perform(get(userUri.toString()))
                .andExpect(status().isNoContent());
    }


    @Test
    void testGetAllDeletedUser_Successfully_ReturnsOk() throws Exception {
        List<UserResponseData> userList = new ArrayList<>();
        userList.add(userResponseData);
        when(userService.getAllDeletedUsers()).thenReturn(userList);
        mockMvc.perform(get(userUri.append("/deleted").toString()))
                .andExpect(status().isOk());
    }

    @Test
    void testGetAllDeletedUser_WhenNoUserExists_ReturnsNoContent() throws Exception {
        List<UserResponseData> userList = Collections.emptyList();
        when(userService.getAllDeletedUsers()).thenReturn(userList);
        mockMvc.perform(get(userUri.append("/deleted").toString()))
                .andExpect(status().isNoContent());
    }

    @Test
    void testDeleteUser_WhenUserNotExistsException_ReturnNotFound() throws Exception {
        Mockito.doThrow(new UserNotExistsException(String.format(Constant.USER_NOT_EXISTS, 1L)))
                .when(userService)
                .updateUser(Mockito.any(UpdateUserRequestData.class), any());
        String requestJson = objectMapper.writeValueAsString(updateUserRequestData);
        mockMvc.perform(put(userUri.append("/{userId}").toString(), 1L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isNotFound());
    }

    @Test
    void testGetUser_WhenUserNotExistsException_ReturnNotFound() throws Exception {
        Mockito.doThrow(new UserNotExistsException(String.format(Constant.USER_NOT_EXISTS, 1L)))
                .when(userService)
                .getUser(any());
        mockMvc.perform(get(userUri.append("/{userId}").toString(), 1L))
                .andExpect(status().isNotFound());
    }

    @Test
    void testGetUser_WhenUserExists_ReturnsOkWithUser() throws Exception {
        when(userService.getUser(any())).thenReturn(userResponseData);
        mockMvc.perform(get(userUri.append("/{userId}").toString(), 1L))
                .andExpect(status().isOk());
    }


}