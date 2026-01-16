class Solution {
    public String freqAlphabets(String s) {
        StringBuilder sb=new StringBuilder();
        int i=s.length()-1;
        while(i>=0){
            if(s.charAt(i)=='#'){
                String str1=s.substring(i-2,i);
                int num=Integer.parseInt(str1);
                sb.append(getChar(num));
                i-=3;
            }else{
           String str2=s.substring(i,i+1);
                int num=Integer.parseInt(str2);
                sb.append(getChar(num));
                i--;
}
            
        }
        return sb.reverse().toString();
        
    }
    char getChar(int n){
        char ch= (char)(96+n);
        return ch;
    }
    
}