public class Prob_28 {
    public int strStr(String haystack, String needle) {
        int lengthNeedle = needle.length();
        int lengthHaystack = haystack.length();
        int c=-1;
        if (lengthNeedle <= lengthHaystack){
            for (int i=0; i<lengthHaystack; i++){
                if (i+lengthNeedle<=lengthHaystack && haystack.substring(i,i+lengthNeedle).equals(needle)){
                    c=i;
                    break;
                }
            }
        }
        return c;
    }
}