package daohuei.leetcodelizard;

import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

public class TwoSumTest {
    @Test
    public void testOnTwoSum() {
        assertArrayEquals("HashMap", new int[] { 0, 1 }, TwoSum.twoSumHashMap(new int[] { 2, 7, 11, 15 }, 9));
        assertArrayEquals("HashMap", new int[] { 1, 2 }, TwoSum.twoSumHashMap(new int[] { 3, 2, 4 }, 6));
        assertArrayEquals("HashMap", new int[] { 0, 1 }, TwoSum.twoSumHashMap(new int[] { 3, 3 }, 6));
    }
}