package ru.job4j.ex;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;

public class FactTest {

    @Test (expected = IllegalArgumentException.class)
    public void whenLessZeroThenFinish() {
        Fact.calc(-8);
    }

    @Test
    public void whenGraterZeroThenCalculate() {
        int rsl = Fact.calc(5);
        assertThat(rsl, is (120));
    }

}