package problem709;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ToLowerCaseTest {

    private ToLowerCase solution = new ToLowerCase();

    @DataProvider(name = "dp1")
    private static Object[][] provideData() {
        return new Object[][]{
                {"Hello", "hello"},
                {"here", "here"},
                {"LOVELY", "lovely"}
        };
    }

    @Test(dataProvider = "dp1")
    public void shouldConvertToLowerCase(final String inStr, final String epxStr) {
        var actStr = solution.toLowerCase(inStr);

        assertThat(actStr, is(epxStr));
    }

}