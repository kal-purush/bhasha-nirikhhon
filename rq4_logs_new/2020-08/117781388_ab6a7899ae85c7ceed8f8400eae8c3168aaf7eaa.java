package HashMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SingleNumberTest {

    private SingleNumber solution;

    @Before
    public void setUp() {
        solution = new SingleNumber();
    }

    /**
     * Example 1.
     */
    @Test
    public void testcase1() {
        int[] input = {2, 2, 1};
        int expected = 1;
        assertEquals(expected, solution.singleNumber(input));
    }

    /**
     * Example 2.
     */
    @Test
    public void testcase2() {
        int[] input = {4, 1, 2, 1, 2};
        int expected = 4;
        assertEquals(expected, solution.singleNumber(input));
    }
}