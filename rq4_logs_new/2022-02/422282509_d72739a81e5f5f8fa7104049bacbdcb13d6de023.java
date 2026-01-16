class Solution {
    public boolean rotateString(String s, String goal) {
        String w=s+s;
        if(s.length()!=goal.length())
            return false;
        if(w.indexOf(goal)==-1)
            return false;
        else return true;
    }
}