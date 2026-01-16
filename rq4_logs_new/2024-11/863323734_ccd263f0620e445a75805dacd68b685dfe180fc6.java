class Solution {
    public String makeFancyString(String s) {
       int frequency = 1;
       StringBuilder sb = new StringBuilder();
       sb.append(s.charAt(0));

       char previous = s.charAt(0);
       for(int i = 1; i < s.length(); i++){
            if(s.charAt(i) == previous){
                frequency++;
            } else {
                frequency = 1;
                previous = s.charAt(i);
            }
            if(frequency <= 2){
                sb.append(s.charAt(i));
            }
       }
       return sb.toString();
    }
}