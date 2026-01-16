package problem1689;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class DeciBinarySumTest {

    private DeciBinarySum solution = new DeciBinarySum();

    @DataProvider(name = "dp1")
    private static Object[][] provideData() {
        return new Object[][]{
                {"32", 3},
                {"82734", 8},
                {"27346209830709182346", 9}
        };
    }

    @Test(dataProvider = "dp1")
    public void shouldCountDeciBinaryNumbersThatSumToTarget(final String number, final int expCnt) {
        var actCnt = solution.minPartitions(number);

        assertThat(actCnt, is(expCnt));
    }

}