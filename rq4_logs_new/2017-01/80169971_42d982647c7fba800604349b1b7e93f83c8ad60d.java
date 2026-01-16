import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
 
public class StratisProject {
public static boolean palindromeMultiples(int multiple, String palindrome){
String pattern= "[^\\w']+";
String numRegEx = "^(0|[1-9][0-9]*)$";
String myArray[] = palindrome.split(pattern);
String result = "";
int greater = 0;
Pattern p = Pattern.compile(numRegEx);
for(int i=0; i<myArray.length; i++){
Matcher m = p.matcher(myArray[i]);
if(m.find()){
String myNumString = "";
for(int j=0; j<myArray[i].length(); j++){
myNumString += myArray[i].charAt(j)+"";
int newNumber = Integer.parseInt(myNumString);
if(newNumber%multiple == 0){
if(newNumber > multiple)
greater = newNumber;
System.out.println(greater);
}
}
}else{
String reverse = "";
       for(int k = myArray[i].length()-1; k>=0; k--){
       
reverse += myArray[i].charAt(k);
     if (myArray[i].equals(reverse))
   
 System.out.println(myArray[i]);
       }
}
}
return palindrome.matches(pattern);
}
public static void main (String args[]){
palindromeMultiples(8, "ANNA BAKES 80 CAKES IN THE NOON, 989216011");
}
 
}