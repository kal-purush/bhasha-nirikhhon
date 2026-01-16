public class prob14 {
//     Problem 14
// Binary Strings Problem

// Print all binary strings of size N without consecutive ones.

// "0100"
// "0110"
// "1000"
// "1010"
// "1100"
// "1110"
    public static void main(String[] args) {
        printBinString(3, 0, "");
    }
    public static void printBinString(int n , int lastPlace, String str) {
        // concept 
        // if(lastPlace == 0){
        //     // sit 0 on chair n 
            // try with the string builder
        //     printBinString(n-1, 0, str.append("0"));
        //     printBinString(n-1, 1, str.append("1"));

        // } else{
        //     printBinString(n-1, 0, str.append("0"));
        // }

        //base case
        if(n== 0){
            System.out.println(str);
            return ;
        }

        
        printBinString(n-1,0, str + "0");
        if(lastPlace == 0) {
            printBinString(n -1 ,1,str+"1");
        }
    }
}