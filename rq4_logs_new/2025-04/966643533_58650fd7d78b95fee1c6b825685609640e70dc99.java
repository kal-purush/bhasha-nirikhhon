package org.spiceboys.Travel.Diary.service;

import org.spiceboys.Travel.Diary.model.Country;
import org.spiceboys.Travel.Diary.repository.CountryRepository;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
public class CountryService {
    private final CountryRepository countryRepository;

    public CountryService(CountryRepository countryRepository){
        this.countryRepository = countryRepository;
    }

    public Country createCountry(Country country){
        return countryRepository.save(country);
    }
}