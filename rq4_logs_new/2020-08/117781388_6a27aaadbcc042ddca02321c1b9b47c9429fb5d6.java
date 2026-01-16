package DynamicProgramming;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClimbStairsTest {

    private ClimbStairs solution;

    @Before
    public void setUp() {
        solution = new ClimbStairs();
    }

    /**
     * Example 1.
     */
    @Test
    public void testcase1() {
        int input = 2;
        int expected = 2;
        assertEquals(expected, solution.climbStairs(input));
    }

    /**
     * Example 2.
     */
    @Test
    public void testcase2() {
        int input = 3;
        int expected = 3;
        assertEquals(expected, solution.climbStairs(input));
    }
}