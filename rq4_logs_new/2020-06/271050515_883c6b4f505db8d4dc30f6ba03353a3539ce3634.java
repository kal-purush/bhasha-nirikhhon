package com.kodilla.stream.world;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class WorldTestSuite {
    @Test
    public void testGetPeopleQuantity() {
        //Given
        Country poland = new Country("37970000");
        Country italy = new Country("60360000");
        Country germany = new Country("83002000");

        Country china = new Country("1393000000");
        Country russia = new Country("144500000");
        Country japan = new Country("126500000");

        Country usa = new Country("327200000");
        Country canada = new Country("37590000");
        Country mexico = new Country("126200000");

        Continent europe = new Continent();
        europe.addCountry(poland);
        europe.addCountry(italy);
        europe.addCountry(germany);

        Continent asia = new Continent();
        asia.addCountry(china);
        asia.addCountry(russia);
        asia.addCountry(japan);

        Continent america = new Continent();
        america.addCountry(usa);
        america.addCountry(canada);
        america.addCountry(mexico);

        World world = new World();
        world.addContinent(europe);
        world.addContinent(asia);
        world.addContinent(america);

        //When
        BigDecimal totalPopulation = world.getContinentList().stream()
                .flatMap(continent -> continent.getCountryList().stream())
                .map(Country::getPeopleQuantity)
                .reduce(BigDecimal.ZERO, (sum, current) -> sum = sum.add(current));

        //When
        BigDecimal expectedSand = new BigDecimal("2336322000");
        Assert.assertEquals(expectedSand, totalPopulation);

    }
}