class Solution {
    public int[] maximumBeauty(int[][] items, int[] queries) {

        Arrays.sort(items, (a, b) -> a[0] - b[0]);

        for(int i = 1; i < items.length; i++){
            items[i][1] = Math.max(items[i][1], items[i-1][1]);
        }

        for(int i = 0; i < queries.length; i++){
            queries[i] = binarySearch(items, queries[i]);
        }

        return queries;
    }

    private int binarySearch(int[][] items, int target){
        int max = 0;
        int left = 0, right = items.length - 1;
        while(left <= right){
            int mid = (left + right) / 2;
            if(items[mid][0] > target){
                right = mid - 1;
            } else {
                max = Math.max(max, items[mid][1]);
                left = mid + 1;
            }
        }
        return max;
    }

}