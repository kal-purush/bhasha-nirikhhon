class Solution {
    public long wonderfulSubstrings(String word) {
        long answer = 0;
        long[] count = new long[1024];
        int mask = 0;
        
        count[0] = 1;
        
        for (char c : word.toCharArray()) {
            int index = c - 'a';
            mask ^= (1 << index);
            answer += count[mask];
            
            for (int i = 0; i < 10; i++) {
                int k = mask ^ (1 << i);
                answer +=count[k];
            }
            count[mask]++;
        }
        
        return answer;
    }
}