package com.github.firulapp.service;

import com.github.firulapp.dto.CityDto;
import com.github.firulapp.exceptions.CityException;

import java.util.List;

public interface CityService {

    List<CityDto> getAllCities();

    List<CityDto> getCitiesByCountry(String country);

    List<CityDto> getCitiesByDistrictAndCountry(String district, String country);

    CityDto getCityById(Long id) throws CityException;

    CityDto saveCity(CityDto cityDto) throws CityException;

    void deleteCity(CityDto cityDto);
}