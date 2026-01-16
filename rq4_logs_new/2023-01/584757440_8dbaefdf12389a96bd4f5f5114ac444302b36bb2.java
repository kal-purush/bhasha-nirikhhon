package Section2;

import java.util.Scanner;

public class Test07 {

    public int solution(int n, int[] arr){
        int cnt =0, sum =0;
        for(int i=0;i<n;i++){
            if(arr[i] == 1){
                cnt++;
                sum += cnt;
            }else{
                cnt=0;
            }
        }
        return sum;
    }

    public static void main(String[] args) {
        Test07 t = new Test07();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[] arr = new int[n];
        for(int i =0;i<n;i++){
            arr[i] = sc.nextInt();
        }
        System.out.println(t.solution(n, arr));
    }
}