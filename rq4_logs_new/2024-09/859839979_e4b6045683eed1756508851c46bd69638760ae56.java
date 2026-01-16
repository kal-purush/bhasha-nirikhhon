package com.fitastic.service;

import com.fitastic.entity.DefaultExercise;
import com.fitastic.repository.DefaultExerciseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class DefaultExerciseServiceTest {

    @Mock
    private DefaultExerciseRepository defaultExerciseRepository;

    @InjectMocks
    private DefaultExerciseService defaultExerciseService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void shouldReturnAllDefaultExercisesWithSuccess(){

        DefaultExercise exercise1 = new DefaultExercise();
        exercise1.setId("1");
        exercise1.setName("Exercise1");
        exercise1.setTarget("Upper body");

        DefaultExercise exercise2 = new DefaultExercise();
        exercise2.setId("2");
        exercise2.setName("Exercise2");
        exercise2.setTarget("Lower body");

        List<DefaultExercise> mockExercises = new ArrayList<>();
        mockExercises.add(exercise1);
        mockExercises.add(exercise2);

        when(defaultExerciseRepository.findAll()).thenReturn(mockExercises);

        List<DefaultExercise> result = defaultExerciseService.getAll();

        assertEquals(2, result.size());
        assertEquals("Exercise1", result.get(0).getName());
        assertEquals("Exercise2", result.get(1).getName());
    }

    @Test
    public void shouldReturnDefaultExerciseById() {
        DefaultExercise exersise = new DefaultExercise();
        exersise.setId("1");
        exersise.setName("Exercise");
        exersise.setTarget("Upper body");

        when(defaultExerciseRepository.findById("1")).thenReturn(Optional.of(exersise));

        DefaultExercise result = defaultExerciseService.getDefaultExerciseById("1");

        assertNotNull(result);
        assertEquals("Exercise", result.getName());
        assertEquals("Upper body", result.getTarget());
    }

}