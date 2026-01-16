

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class L2022112842_18_Test {

    @Test
    void test_zero() {
        int[] nums1 = {1,0,2};
        int[] ret1 = {0,2,0};
        assertEquals(ret1, Solution18.productExceptSelf(nums1));
        assertEquals(ret1, Solution18.productExceptSelf(nums1));//测试简洁明确
    }
}