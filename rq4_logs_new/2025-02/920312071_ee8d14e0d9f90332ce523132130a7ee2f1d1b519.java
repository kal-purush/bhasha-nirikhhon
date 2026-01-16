package _0209.recur;

public class Main {

    static int arr[] = {10, 20, 30};

    private static void printArray2(int index){
        // basis part
        if(index == arr.length){
            return;
        }
        // Inductive part
        System.out.println(arr[index] + " ");

        printArray2(index + 1);
    }

    static void printStar(int n){
        //basis
        if(n == 0) {

            return;
        }

        // inductive

        printStar(n - 1);

        for (int i = 0; i < n; i++) {
            System.out.print("*");
        }
        System.out.println();



    }

    public static void main(String[] args) {
        printArray2(0);

        System.out.println();

        printStar(4);
    }

}