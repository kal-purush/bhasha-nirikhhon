package priv.just.framework.algorithm.midoftwoarrays;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class FirstSolution implements Solution {

    @Override
    public int invoke(int[] arr1, int[] arr2) {
        int m = arr1.length;
        int n = arr2.length;
        int l = (m + n + 1) / 2;
        int r = (m + n + 2) / 2;
        return (findKth(arr1, 0, m - 1, arr2, 0, n - 1, l) + findKth(arr1, 0, m - 1, arr2, 0, n - 1, r)) / 2;
    }

    private int findKth(int[] arr1, int s1, int e1, int[] arr2, int s2, int e2, int k) {
        int l1 = e1 - s1 + 1;
        int l2 = e2 - s2 + 1;
        if (l1 > l2) {
            return findKth(arr2, s2, e2, arr1, s1, e1, k);
        }
        if (l1 == 0) {
            return arr2[s2 + k - 1];
        }
        if (k == 1) {
            return Math.min(arr1[s1], arr2[s2]);
        }
        int i = s1 + Math.min(l1, k / 2) - 1;
        int j = s2 + Math.min(l2, k / 2) - 1;
        if (arr1[i] > arr2[j]) {
            return findKth(arr1, s1, e1, arr2, j + 1, e2, k - (j - s2 + 1));
        } else {
            return findKth(arr1, i + 1, e1, arr2, s2, e2, k - (i - s1 + 1));
        }
    }

    @Test
    public void test() {
        int[] arr1 = {1,3,4,7};
        int[] arr2 = {1,2,3,4,5,6,7,8,9,10};
        int res = invoke(arr1, arr2);
        log.info(String.valueOf(res));
    }

}