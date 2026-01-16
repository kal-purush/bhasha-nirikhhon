import java.util.*;
class Modul1{
    public static void main(String[] args) {
        Scanner n = new Scanner(System.in);
        int N = n.nextInt();

        int elem[] = new int[N];
        for (int i = 0; i < N; i++){
            int batas = n.nextInt();
            elem[i] = batas;
        }
        for (int i = 0; i < N-1; i++){
            for (int j = i; j < N; j++){
                int a = elem[i];
                int b = elem[j];
                int batas;

                while (b != 0){
                    //jadi maksudnya misal 2 dibagi 7 ada sisanya dan 7 dibagi 2 ada sisanya makanya di cetak
                    batas = a % b;
                    a = b;
                    b = batas;
                }
                if (a == 1){
                    System.out.println(elem[i] + " " + elem[j]);
                }
            }
        } 
    }
}