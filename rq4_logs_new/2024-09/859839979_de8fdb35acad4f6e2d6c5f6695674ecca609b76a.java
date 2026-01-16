package com.fitastic.controller;

import com.fitastic.entity.UserExercise;
import com.fitastic.service.UserExerciseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@SpringBootTest
@AutoConfigureMockMvc
class UserExerciseControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private UserExerciseService userExerciseService;

    @Test
    void shouldReturnAllUserExercises() throws Exception{
        UserExercise exercise1 = new UserExercise();
        exercise1.setId("1");
        exercise1.setName("Push-up");
        exercise1.setUserId("1");

        UserExercise exercise2 = new UserExercise();
        exercise2.setId("2");
        exercise2.setName("Squat");
        exercise2.setUserId("1");


        when(userExerciseService.getAll()).thenReturn(Arrays.asList(exercise1, exercise2));

        mockMvc.perform(get("/api/userExercises"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$[0].id").value("1"))
                .andExpect(jsonPath("$[0].name").value("Push-up"))
                .andExpect(jsonPath("$[0].userId").value("1"))
                .andExpect(jsonPath("$[1].id").value("2"))
                .andExpect(jsonPath("$[1].name").value("Squat"))
                .andExpect(jsonPath("$[1].userId").value("1"));
    }

    @Test
    void shouldCreateUserExercise() throws Exception {
        UserExercise exercise = new UserExercise();
        exercise.setId("1");
        exercise.setName("Push-up");
        exercise.setUserId("1");

        when(userExerciseService.createUserExercise(any(UserExercise.class))).thenReturn(exercise);

        String exerciseJson = "{\"id\":\"1\",\"name\":\"Push-up\",\"userId\" : \"1\"}";

        mockMvc.perform(post("/api/userExercises")
                .contentType(MediaType.APPLICATION_JSON)
                .content(exerciseJson))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/exercise/1"))
                .andExpect(jsonPath("$.id").value("1"))
                .andExpect(jsonPath("$.name").value("Push-up"))
                .andExpect(jsonPath("$.userId").value("1"));

    }

    @Test
    void shouldUpdateUserExercise() throws Exception {
        UserExercise exercise = new UserExercise();
        exercise.setId("1");
        exercise.setName("Push-up");
        exercise.setUserId("1");

        UserExercise exerciseUpdated = new UserExercise();
        exerciseUpdated.setId("1");
        exerciseUpdated.setName("Squat");
        exerciseUpdated.setUserId("1");

        when(userExerciseService.updateUserExercise("1", exerciseUpdated)).thenReturn(exerciseUpdated);

        mockMvc.perform(patch("/api/userExercises/1")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\": \"1\", \"name\": \"Squat\", \"userId\" : \"1\"}"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value("1"))
                .andExpect(jsonPath("$.name").value("Squat"))
                .andExpect(jsonPath("$.userId").value("1"));
    }

    @Test
    void shouldReturnUserExercise() throws Exception {

        UserExercise exercise = new UserExercise();
        exercise.setId("1");
        exercise.setName("Push-up");
        exercise.setUserId("1");

        when(userExerciseService.getUserExerciseById("1")).thenReturn(exercise);

        mockMvc.perform(get("/api/userExercises/1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value("1"))
                .andExpect(jsonPath("$.name").value("Push-up"))
                .andExpect(jsonPath("$.userId").value("1"));
    }

    @Test
    void shouldDeleteUserExercise() throws Exception {

        UserExercise exercise = new UserExercise();
        exercise.setId("1");
        exercise.setName("Push-up");
        exercise.setUserId("1");

        doNothing().when(userExerciseService).deleteUserExerciseById("1");

        mockMvc.perform(delete("/api/userExercises/1"))
                .andExpect(status().isNoContent());

        verify(userExerciseService, times(1)).deleteUserExerciseById("1");
    }
}