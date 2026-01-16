class Solution {
    public String compressedString(String word) {
        StringBuilder compressed = new StringBuilder();
        int i = 0;
        while(i < word.length()) {
            char currentChar = word.charAt(i);
            int count = 0;
            while(i < word.length() && count < 9 && word.charAt(i) == currentChar){
                i++;
                count++;
            }
            compressed.append(count).append(currentChar);
        }
        return compressed.toString();
    }
}