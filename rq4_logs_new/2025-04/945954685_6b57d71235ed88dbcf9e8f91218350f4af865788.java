package algorithm.bitA_Algorithm;

import java.util.Scanner;

public class dfs {

    public static void main(String[] args) {


        //func(0, 10);
        //System.out.println(fibo(100));
        ggg(10, 11, 12);
    }

    static void func(int n, int end){

        if (n == end){
            return; // 스택을 뱉는다
        }

        System.out.println(n);

        func(n+1, end); //스택을 쌓는다
    }

    static int fibo(int n){
        if (n <= 1){
            return 1;
        }

        return fibo(n-1) + fibo(n-2);
    }

    static void ggg(int a, int b, int c){

        long hello = a;

        for (int i = 0; i < b; i++){
            hello *= a;
        }

        System.out.println(hello%c);
    }
}

class multiple{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        long a = sc.nextLong();
        long b = sc.nextLong();
        long c = sc.nextLong();

        long hello = 1;

        for (int i = 0; i < b; i++){
            hello *= a;
        }

        System.out.println(hello%c);
    }
}

class Square{

    static int cnt = 0;

    public static void main(String[] args) {
        //수를 입력받기
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int s = sc.nextInt();
        int[] array = new int[n];

        for (int i = 0; i < 5; i++){
            array[i] = sc.nextInt();
        }
        //입력받은 숫자로 가능한 순열을 전부 구하고 만약 계산 값이 S와 같다면 결과++

    }

    public static void bt(int[] array, int s, boolean[] check, int result){

        if (result == s){
            cnt++;
            return;
        }



        for (int i = 0; i < array.length; i++){

            if (!check[i]){
                check[i] = false;




            }








        }


    }



}