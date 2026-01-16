package com.codecool.solarwatch.service;

import com.codecool.solarwatch.model.*;
import com.codecool.solarwatch.model.entity.City;
import com.codecool.solarwatch.model.entity.SunriseSunset;
import com.codecool.solarwatch.repository.CityRepository;
import com.codecool.solarwatch.repository.SunriseSunsetRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SolarWatchServiceTest {
    private SolarWatchService solarWatchService;
    @Mock
    private RestTemplate restTemplate;

    @Mock
    private CityRepository cityRepository;
    @Mock
    private SunriseSunsetRepository sunriseSunsetRepository;
    private City city;
    private SunriseSunset sunriseSunset;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        solarWatchService = new SolarWatchService(restTemplate, cityRepository, sunriseSunsetRepository);

        city = new City();
        city.setName("London");
        city.setState("England");
        city.setCountry("GB");
        city.setLat(51.5073219);
        city.setLon(-0.1276474);

        sunriseSunset = new SunriseSunset();
        sunriseSunset.setCity(city);
        sunriseSunset.setSunrise("5:30:00 AM");
        sunriseSunset.setSunset("19:40:00 PM");
        sunriseSunset.setDate(LocalDate.of(2024, 8, 16));
    }

    @Test
    public void testGetAllCities() {
        Mockito.when(cityRepository.findAll()).thenReturn(List.of(city));
        Cities cities = new Cities(List.of(city.getName()));
        assertEquals(cities, solarWatchService.getAllCities());
    }

    @Test
    public void testGetAllDatesForCity() {
        Mockito.when(cityRepository.findByName("London")).thenReturn(Optional.of(city));
        Mockito.when(sunriseSunsetRepository.findAll()).thenReturn(List.of(sunriseSunset));

        Dates expectedDates = new Dates(List.of(sunriseSunset.getDate()));
        Dates actualDates = solarWatchService.getAllDatesForCity("London");
        assertEquals(expectedDates, actualDates);
    }

    @Test
    public void testAddNewSunsetSunriseForCity() {
        Mockito.when(cityRepository.findByName("London")).thenReturn(Optional.of(city));
        Mockito.when(sunriseSunsetRepository.findByCityAndDate(city, sunriseSunset.getDate()))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(sunriseSunset));

        Mockito.when(restTemplate.getForEntity(Mockito.anyString(), Mockito.eq(CityReport[].class)))
                .thenReturn(ResponseEntity.ok(new CityReport[]{new CityReport(city.getLat(), city.getLon(), city.getName(), city.getState(), city.getCountry())}));

        Mockito.when(restTemplate.getForEntity(Mockito.anyString(), Mockito.eq(SolarReportResult.class)))
                .thenReturn(ResponseEntity.ok(new SolarReportResult(new SolarReportInsideResult(sunriseSunset.getSunrise(), sunriseSunset.getSunset()))));

        SolarReport expectedReport = new SolarReport(sunriseSunset.getDate(), sunriseSunset.getSunset(), sunriseSunset.getSunrise(), city.getName());
        SolarReport actualReport = solarWatchService.addNewSunsetSunriseForCity("London", sunriseSunset.getDate());
        assertEquals(expectedReport, actualReport);
    }

    @Test
    public void testDeleteSunriseSunriseByCityName() {
        Mockito.when(cityRepository.findByName("London")).thenReturn(Optional.of(city));

        solarWatchService.deleteSunriseSunriseByCityName("London");
        Mockito.verify(cityRepository, Mockito.times(1)).delete(city);
    }

    @Test
    public void testGetSunsetSunriseForCity() {
        Mockito.when(cityRepository.findByName("London")).thenReturn(Optional.of(city));
        Mockito.when(sunriseSunsetRepository.findByCityAndDate(city, sunriseSunset.getDate()))
                .thenReturn(Optional.of(sunriseSunset));

        SolarReport expectedReport = new SolarReport(sunriseSunset.getDate(), sunriseSunset.getSunset(), sunriseSunset.getSunrise(), city.getName());
        SolarReport actualReport = solarWatchService.getSunsetSunriseForCity("London", sunriseSunset.getDate());
        assertEquals(expectedReport, actualReport);
    }

    @Test
    public void testGetCityNotFoundThrowsException() {
        Mockito.when(cityRepository.findByName("NonExistentCity")).thenReturn(Optional.empty());
        assertThrows(NoSuchElementException.class, () -> solarWatchService.getSunsetSunriseForCity("NonExistentCity", LocalDate.now()));
    }



}