package TestSg;

import java.util.LinkedList;

public class KataTest {

       public static void main(String[] args) {

              for (int i = 1; i <= 100; i++) {
                     String res = "";
                     if ((i % 3) == 0) {
                         res = res + "Foo";
                     }                   

                     if ((i % 5) == 0) {
                         res = res + "Bar";
                     }
               
                     if ((i % 7) == 0) {
                         res = res + "Qix";
                     }
                  
                     int number = i; // = and int
                     LinkedList<Integer> stack = new LinkedList<Integer>();
                     while (number > 0) {
                        stack.push( number % 10  ); // insertion du reste de la division par 10
                        number = number / 10; // diviser le num�ro par 10
                     }
                     //
                     while (!stack.isEmpty()) { //la methode qui verifie si le stack est vide ou pas
                        int digit = stack.pop(); //

                        if(digit == 3){
                            res = res +"Foo";
                        }
                        
                        if(digit == 5){
                            res = res +"Bar";
                        }
                        
                        if(digit == 7){
                            res = res +"Qix";
                        }
                     }

                     if(res.length() == 0){
                           System.out.println(i);
                     }

                     else{
                    	 System.out.println(res);
                     }
              }
       }
}