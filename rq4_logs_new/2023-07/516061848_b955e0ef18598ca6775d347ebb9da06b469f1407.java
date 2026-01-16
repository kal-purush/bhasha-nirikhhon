package Easy._344_Reverse_String;

public class Solution {

    public static void reverseString(char[] s) {
     int a_pointer=0;
     int b_pointer= s.length-1;

     while (a_pointer<= b_pointer) {

      char temp = s[a_pointer];
      s[a_pointer] = s[b_pointer];
      s[b_pointer] = temp;

         a_pointer++;
         b_pointer--;
     }
      }

    public static void main(String[] args) {
        char[] array = {'h', 'e', 'l', 'l', 'o' };

        System.out.println(array);

        reverseString(array);

        System.out.println(array);

    }
}