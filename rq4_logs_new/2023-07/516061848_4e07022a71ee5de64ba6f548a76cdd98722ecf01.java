package Easy._67_Add_Binary;

public class Solution {

//    Input: a = "11", b = "1"
//    Output: "100"

    public static String addBinary(String a, String b) {
       StringBuilder sb = new StringBuilder();

       int i = a.length()-1;
       int j = b.length()-1;
       int carry =0;


        while (i >= 0 || j >= 0) {
            int sum = carry;
            if(i>=0) sum += a.charAt(i) - '0';
            if(j>=0) sum += b.charAt(j) - '0';
            sb.append(sum%2);
            carry = sum/2;

            i--;
            j--;
        }

        if(carry !=0) sb.append(carry);
        return sb.reverse().toString();

    }


    public static void main(String[] args) {
        System.out.println(addBinary("11", "1"));


    }


}