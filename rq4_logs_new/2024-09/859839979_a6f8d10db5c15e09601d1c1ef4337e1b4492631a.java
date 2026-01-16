package com.fitastic.service;

import com.fitastic.entity.UserExercise;
import com.fitastic.repository.UserExerciseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class UserExerciseServiceTest {

    @Mock
    private UserExerciseRepository userExerciseRepository;

    @InjectMocks
    private UserExerciseService userExerciseService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void shouldReturnAllUserExercises() {
        UserExercise userExercise1 = new UserExercise();
        userExercise1.setUserId("1");
        userExercise1.setName("Exercise1");
        userExercise1.setInstructions(new String[]{"Upper body"});

        UserExercise userExercise2 = new UserExercise();
        userExercise2.setUserId("2");
        userExercise2.setName("Exercise2");
        userExercise2.setInstructions(new String[]{"Lower body"});

        List<UserExercise> mockExercises = new ArrayList<>();
        mockExercises.add(userExercise1);
        mockExercises.add(userExercise2);
        
        when(userExerciseRepository.findAll()).thenReturn(mockExercises);

        List<UserExercise> result = userExerciseService.getAll();

        assertEquals(2, result.size());
        assertEquals("Exercise1", result.get(0).getName());
        assertEquals("Upper body", result.get(0).getInstructions()[0]);
        assertEquals("Exercise2", result.get(1).getName());
        assertEquals("Lower body", result.get(1).getInstructions()[0]);

    }

    @Test
    void shouldReturnUserExerciseById() {
        UserExercise userExercise = new UserExercise();

        userExercise.setUserId("1");
        userExercise.setName("Exercise");
        userExercise.setInstructions(new String[]{"Upper body"});

        when(userExerciseRepository.findById("1")).thenReturn(Optional.of(userExercise));

        UserExercise result = userExerciseService.getUserExerciseById("1");

        assertNotNull(userExercise);
        assertEquals("Exercise", result.getName());
        assertEquals("Upper body", result.getInstructions()[0]);
    }

    @Test
    void shouldReturnUpdateUserExercise() {
        UserExercise userExercise = new UserExercise();
        userExercise.setUserId("1");
        userExercise.setName("Exercise");
        userExercise.setInstructions(new String[]{"Upper body"});

        UserExercise updatedExercise = new UserExercise();
        updatedExercise.setUserId("2");
        updatedExercise.setName("ExerciseUpdated");
        updatedExercise.setInstructions(new String[]{"Lower body"});

        when(userExerciseRepository.findById("1")).thenReturn(Optional.of(userExercise));

        when(userExerciseRepository.save(any(UserExercise.class))).thenReturn(updatedExercise);

        UserExercise result = userExerciseService.updateUserExercise("1", updatedExercise );

        assertNotNull(userExercise);
        assertEquals("2", result.getUserId());
        assertEquals("ExerciseUpdated", result.getName());
        assertEquals("Lower body", result.getInstructions()[0]);
    }

    @Test
    void shouldReturnDeleteUserExerciseById() {
        UserExercise userExercise = new UserExercise();
        userExercise.setUserId("1");
        userExercise.setName("Exercise");
        userExercise.setInstructions(new String[]{"Upper body"});

        when(userExerciseRepository.existsById("1")).thenReturn(true);

        doNothing().when(userExerciseRepository).deleteById("1");

        userExerciseService.deleteUserExerciseById("1");

        verify(userExerciseRepository, times(1)).deleteById("1");

        verify(userExerciseRepository, times(1)).existsById("1");
    }
}