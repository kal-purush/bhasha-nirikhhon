package problem1480;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RunningSumTest {

    private RunningSum solution = new RunningSum();

    @DataProvider(name = "dp1")
    private static Object[][] provideData() {
        return new Object[][]{
                {new int[]{1, 2, 3, 4}, new int[]{1, 3, 6, 10}},
                {new int[]{1, 1, 1, 1, 1}, new int[]{1, 2, 3, 4, 5}},
                {new int[]{3, 1, 2, 10, 1}, new int[]{3, 4, 6, 16, 17}}
        };
    }

    @Test(dataProvider = "dp1")
    public void shouldCalcRunningSum(final int[] inArr, final int[] expArr) {
        var actArr = solution.runningSum(inArr);

        assertThat(actArr, is(expArr));
    }

}