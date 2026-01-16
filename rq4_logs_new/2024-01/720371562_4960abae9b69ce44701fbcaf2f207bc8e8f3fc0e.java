package com.example.backend.services;

import com.example.backend.models.AlmostCityEntity;
import com.example.backend.repositories.AlmostCityRepo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlmostCityServiceTest {

    @Mock
    private AlmostCityRepo almostCityRepo;

    @InjectMocks
    private AlmostCityService almostCityService;
    @Test
    void loadSampleCities() {
        almostCityService.loadSampleCities();
        verify(almostCityRepo, times(1)).sampleCities();
    }

    @Test
    void loadSampleCapitals() {
        almostCityService.loadSampleCapitals();
        verify(almostCityRepo, times(1)).sampleCapitals();
    }
}