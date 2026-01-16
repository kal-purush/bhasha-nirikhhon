class WordBreak {
    public boolean wordBreak(String s, List<String> wordDict) {
        return wordBreak(s,new HashSet<>(wordDict),0,new Boolean[s.length()]);

    }

    public boolean wordBreak(String s, Set<String> set, int index, Boolean[] memo) {

        if(s.length() == index)
            return true;

        if(memo[index] != null)
            return memo[index];

        int n = s.length();

        for(int i = index+1; i <= n; i++) {
            String currWord = s.substring(index,i);
            if(set.contains(currWord) && wordBreak(s,set,i,memo))
                return memo[index] = true;
        }
        return memo[index] = false;
    }
}

// simple recursion technique. where we iterate through the string. if a word is found, then recurse with remaning length.
// increment the index.
// here a same word can be sent into function many time, to avoid, we use memoization techinque to store true or false
// at that particular index.
//time complexity: O(n2)
// space complexity O(n)