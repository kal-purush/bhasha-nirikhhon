package com.university.contractors.service;

import com.university.contractors.model.Country;
import com.university.contractors.repository.CountryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.List;
import java.util.Optional;

@Service
public class CountryService {

    private static final String LOCAL_COUNTRY_ENG_NAME = "Ukraine";

    private final CountryRepository countryRepository;

    @Autowired
    public CountryService(CountryRepository countryRepository) {
        this.countryRepository = countryRepository;
    }


    public Country getLocalCountry() {
        final List<Country> conuntryList = countryRepository.findByCountryNameEng(LOCAL_COUNTRY_ENG_NAME);

        if (conuntryList.size() > 1) {
            throw new InvalidStateException("There more than one country with name: '" + LOCAL_COUNTRY_ENG_NAME + "'");
        }

        final Optional<Country> optionalCountry = conuntryList.stream().findFirst();

        if (!optionalCountry.isPresent()) {
            throw new RuntimeException("Local country with name: '" + LOCAL_COUNTRY_ENG_NAME + "' was not found in the database");
        }

        return optionalCountry.get();
    }
}