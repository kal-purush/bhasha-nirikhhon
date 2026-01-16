import java.lang.System;

public class FirstMissingPositive {
    public static int firstMissingPositive(int[] A) {
        if(A.length == 0)
            return 1;
        for(int i = 0; i < A.length; i++){
            if(A[i] > 0 && A[i] - 1 < A.length && A[i] - 1 != i && A[i] != A[A[i] - 1]) {
                int t = A[A[i] - 1];
                A[A[i] - 1] = A[i];
                A[i] = t;
                i--;
            }
        }
        for(int j = 0; j < A.length; j++){
            if(A[j] - 1 != j)
                return j + 1;
        }
        return A.length + 1;
    }

    public static void main(String[] args) {
        int[] A ={5,6,7};
        System.out.print(firstMissingPositive(A));
    }

}