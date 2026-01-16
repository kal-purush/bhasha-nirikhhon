// Time Complexity : O(p + s), p, s is length of the two strings
// Space Complexity : O(1)
// Did this code successfully run on Leetcode : Yes
// Any problem you faced while coding this : No

// Notes : Create a HashMap with all characters and their frequencies from string p. Iterate over the characters from string s, and create a sliding window of length s, and check if all the characters in that sliding window have the characters in the HashMap by decreasing the count if present. When the limits of the window are changed, adjust the HashMap values again by adding the character that doesnt exist in the window anymore. Thus, at a particular time the HashMap will be the representative of how many characters of the string p are present in the window. A HashMap with all values of zero will mean, that we have found the anagram of string p.
  
public class FindAllAnagramsInAString {
    public List<Integer> findAnagrams(String s, String p) {
        List<Integer> result = new ArrayList<>();
        if(s.length() == 0) return result;
        
        HashMap<Character, Integer> map = new HashMap<>();
        
        for(int i = 0; i < p.length(); i++){
            map.put(p.charAt(i) , map.getOrDefault(p.charAt(i), 0) + 1);
        }
        
        int totalLength = 0;
        for(int i = 0; i < s.length(); i++){
            char curr = s.charAt(i);
            
            if(map.containsKey(curr)){
                map.put(curr , map.get(curr) - 1);

                if(map.get(curr) == 0){
                    totalLength = totalLength + 1;
                }
            }
            
            if(i >= p.length()){
                char prev = s.charAt(i - p.length());
                if(map.containsKey(prev)){
                    map.put(prev , map.get(prev) + 1);

                    if(map.get(prev) == 1){
                        totalLength = totalLength - 1;
                    }
                }
            }
            
            if(totalLength == map.size()){
                result.add(i + 1 - p.length());
            }
        }
        
        return result;
    }
}