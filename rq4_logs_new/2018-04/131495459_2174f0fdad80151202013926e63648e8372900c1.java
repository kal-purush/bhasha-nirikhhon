package com.assignment.countryfinderrest.unit;


import com.assignment.countryfinderrest.config.CountryFinderException;
import com.assignment.countryfinderrest.dao.CountryFinderDao;
import com.assignment.countryfinderrest.model.CountryRequest;
import com.assignment.countryfinderrest.model.CountryResponse;
import com.assignment.countryfinderrest.service.CountryFinderService;
import com.assignment.countryfinderrest.utility.CaseConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CountryFinderServiceTest {

    @InjectMocks
    CountryFinderService countryFinderService ;

    @Mock
    CountryFinderDao countryFinderDao;


    List<CountryResponse> countryResponse = new ArrayList<>();

  /*  @Before
    public void beforeTest()  {
    }*/

    @Test
    public void testGetCountryList() throws CountryFinderException {
        CountryRequest countryRequest =  new CountryRequest();
        countryRequest.setSearchText("Ind");
        when(countryFinderDao.getCountryList(CaseConverter.camelCase(countryRequest.getSearchText()))).thenReturn(countryResponse);
        countryFinderService.getCountryList(countryRequest);

    }
    @Test
    public void testGetCountryWithMaxPopulation() throws CountryFinderException {
        when(countryFinderDao.getCountryWithMaxPopulation()).thenReturn(countryResponse);
        countryFinderService.getCountryWithMaxPopulation();

    }
    @Test
    public void testGetCountryWithMinPopulation() throws CountryFinderException {
        when(countryFinderDao.getCountryWithMinPopulation()).thenReturn(countryResponse);
        countryFinderService.getCountryWithMinPopulation();

    }
    @Test
    public void testGetMaxCurrenciesUsed() throws CountryFinderException {
        when(countryFinderDao.getMaxCurrenciesUsed()).thenReturn(countryResponse);
        countryFinderService.getMaxCurrenciesUsed();

    }


}
