package Lab21_1;

import java.util.Scanner;

public class Lab21_1 {
    public static void main(String[] args){
        Scanner in = new Scanner(System.in);
        System.out.print("Please Enter a Number: ");
        int num = in.nextInt();
        toBinary(num);
    }
    public static void toBinary(int num){
      String binary = "";
      for(int i=0; i<8; i++){
          if(num % 2 == 1){
              binary = '1'+binary;
          }
          if(num % 2 == 0){
              binary = '0'+binary;
          }
          num = num/2;
      }
      for(int i=0; i<binary.length(); i++) {
          if ((binary.charAt(0))=='0') {
              binary = binary.substring(1);
          }
      }
      System.out.println("Binary Value is: "+binary);
    }
}