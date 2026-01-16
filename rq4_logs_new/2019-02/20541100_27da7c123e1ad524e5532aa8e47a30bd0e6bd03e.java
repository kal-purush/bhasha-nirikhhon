package pl.kubiczak.orrrg.test.orrrgtest;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class HelloTest {

  @Test
  public void shouldCreateMessage() {
    //Given
    Hello tested = new Hello();
    //When
    String actual = tested.getMessage("<name>");
    //Then
    assertThat(actual, equalTo("Hello <name>!"));
  }
}