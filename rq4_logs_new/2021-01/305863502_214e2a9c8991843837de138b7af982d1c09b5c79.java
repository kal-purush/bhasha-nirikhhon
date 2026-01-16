import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestRoutines_checkIsOneAndFourArray {

    private int[] values;
    boolean expected;

    public TestRoutines_checkIsOneAndFourArray(int[] values, boolean expected) {
        this.values = values;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {new int[] { 1, 1, 1, 4, 4, 1, 4, 4}, true},
                {new int[] { 1, 1, 1, 1, 1, 1}, false},
                {new int[] { 4, 4, 4, 4}, false},
                {new int[] { 1, 4, 4, 1, 1, 4, 3}, false}
        });
    }

    @Test
    public void testCheckIsOneAndFourArray() {
        Assert.assertEquals(expected, Routines.checkIsOneAndFourArray(values));
    }
}