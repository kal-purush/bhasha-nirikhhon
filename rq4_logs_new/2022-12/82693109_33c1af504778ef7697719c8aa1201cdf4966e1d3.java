package leetcode.p1704;

import java.util.Set;

// 2 <= s.length <= 1000
// s.length is even.
// s consists of uppercase and lowercase letters.
class Solution {
    // No need to actually split the array to compare left and right halves
    public boolean halvesAreAlike(String s) {
        final Set<Character> vowels = Set.of('a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U');
        final int n = s.length();
        final int r = n / 2; // a head index of the right half
        int cnt = 0;
        for (int i = 0; i < n; i++) {
            if (vowels.contains(s.charAt(i))) {
                if (i < r) {
                    cnt++;
                } else {
                    cnt--;
                }
            }
        }
        return cnt == 0;
    }
}