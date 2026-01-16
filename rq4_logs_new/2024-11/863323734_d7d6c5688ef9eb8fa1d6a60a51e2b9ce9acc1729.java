public class Solution {
    public int minimizedMaximum(int n, int[] quantities) {
        int left = 1;
        int right = Arrays.stream(quantities).max().getAsInt();

        while (left < right) {
            int mid = left + (right - left) / 2;
            if (canDistribute(quantities, n, mid)) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private boolean canDistribute(int[] quantities, int n, int maxProducts) {
        int requiredStores = 0;
        
        for (int quantity : quantities) {
            requiredStores += (quantity + maxProducts - 1) / maxProducts;
            if (requiredStores > n) {
                return false;
            }
        }
        return requiredStores <= n;
    }
}