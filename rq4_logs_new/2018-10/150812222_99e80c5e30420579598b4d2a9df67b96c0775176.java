package br.com.rbs.brewery.domain;

import org.junit.Assert;
import org.junit.Test;

public class BeerTest {

    @Test
    public void getDifferenceMatch() {
        int expected = 0;
        int temperature = -2;
        Beer beer = new Beer("Weissbier", -4, 3);
        beer.setTargetTemperature(temperature);
        Assert.assertEquals(expected, beer.getDifference());
    }

    @Test
    public void getDifferenceMin() {
        int expected = 2;
        int temperature = -6;
        Beer beer = new Beer("Weissbier", -4, 3);
        beer.setTargetTemperature(temperature);
        Assert.assertEquals(expected, beer.getDifference());
    }

    @Test
    public void getDifferenceMax() {
        int expected = 1;
        int temperature = 4;
        Beer beer = new Beer("Weissbier", -4, 3);
        beer.setTargetTemperature(temperature);
        Assert.assertEquals(expected, beer.getDifference());
    }
}