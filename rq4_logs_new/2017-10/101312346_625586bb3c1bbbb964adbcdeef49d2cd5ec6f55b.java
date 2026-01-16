package io.myproject.firstproject.springbootstarter.statistic;


import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StatisticServiceTest extends TestCase {

  @Autowired
  private StatisticController controller;

  private List<Long> numbers = Arrays.asList(1L, 2L, 3L);

  @After
  public void after() {
    clearStats();
  }

  @Test
  public void checkNumIsNullTest() {
    assertThat(controller.getStatistic()).isNull();
  }

  @Test
  public void checkNumIsEqualTest() {
    addNumbers();
    assertEquals(numbers.size(), controller.getNumberCount());

  }

  @Test
  public void checkGetSumTest() {
    addNumbers();
    assertEquals("Sum = 6.0", controller.getSpecificStatistic("sum"));
  }

  @Test
  public void checkGetAvgTest() {
    addNumbers();
    assertEquals("Average = 2.0", controller.getSpecificStatistic("average"));
  }

  @Test
  public void checkGetMedianTest() {
    addNumbers();
    assertEquals("Median = 2", controller.getSpecificStatistic("median"));
  }

  private void addNumbers() {
    for (long l : numbers) {
      controller.addNumber(l);
    }
  }

  private void clearStats() {
    if (controller != null && controller.getStatistic() != null)
      controller.getStatistic().clear();
  }
}