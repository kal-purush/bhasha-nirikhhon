import java.util.Scanner;
class Nomor1 {
    public static void main(String[] args) {
       Scanner obj = new Scanner(System.in);
       int input = obj.nextInt();
       int[] array = new int[input];

       for (int i = 0; i < input; i++) {
           array[i] = obj.nextInt();
       }
       for (int i = 0; i < array.length; i++) {
            for (int j = i+1; j < array.length; j++) {
               int x = 1;
               for ( int k = 2; k <= (array[j] > array[i] ? array[i] : array[j]); k++) {
                   if (array[j] % k == 0 && array[i] % k == 0)
                   x = k;
               }
            if ( x == 1)
            System.out.printf(" %d %d \n", array[i], array[j]);
            }
       }
    }
}