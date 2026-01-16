public class Subsets {
    

public static void printSubset(String str , String ans ,int i ){

       //baseCase
       if(i==str.length()){

            if(ans.length()==0){
            System.out.println("null");
        }

        else{
            System.out.println(ans);
        }
        
          return;
       }
       //Yes choice
       printSubset(str, ans+str.charAt(i), i+1); //recursion
       //No choice
       printSubset(str, ans, i+1);     //backtrack
}


    public static void main (String args[]){
        String str = "abc";
        printSubset(str, "", 0);
    }
}