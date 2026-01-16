package Array;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SortArrayByParityTest {
    @Mock
    public SortArrayByParity solution;

    @Before
    public void setUp() {
        solution = new SortArrayByParity();
    }

    @Test
    public void testcase1() {
        int[] input = {3, 1, 2, 4};
        int[] expected = {4, 2, 1, 3};
        assertArrayEquals(expected, solution.sortArrayByParity(input));
    }
}