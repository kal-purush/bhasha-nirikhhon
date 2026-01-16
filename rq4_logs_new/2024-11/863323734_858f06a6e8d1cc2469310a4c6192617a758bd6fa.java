class Solution {
    public int takeCharacters(String s, int k) {
        int n = s.length();
        int[] count = new int[3]; // Count occurrences of 'a', 'b', 'c'
        
        for (char c : s.toCharArray()) {
            count[c - 'a']++;
        }
        
        // If any character appears less than k times, it's not possible
        if (count[0] < k || count[1] < k || count[2] < k) {
            return -1;
        }
        
        int minLength = n;
        int[] tempCount = count.clone();
        int l = 0;
        
        // Sliding window to find the smallest valid substring
        for (int r = 0; r < n; r++) {
            tempCount[s.charAt(r) - 'a']--;
            while (tempCount[s.charAt(r) - 'a'] < k) {
                tempCount[s.charAt(l) - 'a']++;
                l++;
            }
            minLength = Math.min(minLength, n - (r - l + 1));
        }
        
        return minLength;
    }
}
