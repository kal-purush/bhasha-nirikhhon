import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MinimalDistanceTest extends AbstractTest {

  String input1;
    String input2;
  int expected;
  Runnable task = new Runnable() {
    @Override
    public void run() {
      int actual = new MinimalDistance().minDistance(input1, input2);
      if (expected != actual) {
        Common.assertEquals(error("\n"+input1+"\n - \n"+input2+"\n"), expected, actual);
      }
    }
  };

  public MinimalDistanceTest(String input1, String input2, int expected) {
    super("MinimalDistance");
    this.input1 = input1;
    this.input2 = input2;
    this.expected = expected;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{
              {"abc", "abcd", 1},
              {"abc", "def", 3},
              {"Monday", "Summer", 6},
              {"Three Rings for the Elven-kings under the sky,\n" +
                      "Seven for the Dwarf-lords in their halls of stone,\n" +
                      "Nine for Mortal Men doomed to die,\n" +
                      "One for the Dark Lord on his dark throne\n" +
                      "In the Land of Mordor where the Shadows lie.",
                      "One Ring to rule them all, One Ring to find them,\n" +
                      "One Ring to bring them all and in the darkness bind them\n" +
                      "In the Land of Mordor where the Shadows lie.", 120},
      });
  }

  @Override
  protected Runnable getTask() {
    return task;
  }

  @Override
  protected String lastInput() {
    return new StringBuilder()
        .append("Input: ")
        .append("\n"+input1+"\n - \n"+input2+"\n")
        .append("\nExpected: ")
        .append("" + expected)
        .toString();
  }
}