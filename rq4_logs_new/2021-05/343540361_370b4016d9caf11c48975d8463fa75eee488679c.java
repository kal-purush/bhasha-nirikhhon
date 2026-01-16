package problem665;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NonDecreasingArrayTest {

    private NonDecreasingArray solution = new NonDecreasingArray();

    @DataProvider(name = "dp1")
    private static Object[][] provideData() {
        return new Object[][]{
                {new int[]{4, 2, 3}, true},
                {new int[]{4, 2, 1}, false},
                {new int[]{3, 4, 2, 3}, false}
        };
    }

    @Test(dataProvider = "dp1")
    public void shouldCheckIfArrayIsNonDecreasing(final int[] arr, final boolean expAnswer) {
        var actAnswer = solution.checkPossibility(arr);

        assertThat(actAnswer, is(expAnswer));
    }
}