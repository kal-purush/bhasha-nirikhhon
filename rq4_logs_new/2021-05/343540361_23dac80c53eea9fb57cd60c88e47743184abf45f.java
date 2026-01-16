package problem462;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MinimumMoves2Test {

    private MinimumMoves2 solution = new MinimumMoves2();

    @DataProvider(name = "dp1")
    private static Object[][] provideData() {
        return new Object[][]{
                {new int[]{1, 2, 3}, 2},
                {new int[]{1, 10, 2, 9}, 16},
                {new int[]{1, 0, 0, 8, 6}, 14}
        };
    }

    @Test(dataProvider = "dp1")
    public void shouldFindMinNumberOfMoves(final int[] nums, final int expMinNum) {
        var actMinNum = solution.minMoves2(nums);

        assertThat(actMinNum, is(expMinNum));
    }
}