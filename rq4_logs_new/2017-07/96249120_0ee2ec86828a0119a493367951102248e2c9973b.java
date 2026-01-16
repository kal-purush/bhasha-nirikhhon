import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {


        Scanner scanner = new Scanner(new File("INPUT.txt"));
        int n = scanner.nextInt()-1;
        int[] storage = new int[n];

        for(int p = 0; p<n;p++){
            storage[p]=scanner.nextInt();
        }
        int result=0;
        for(int k = 1;k<=n;k++){
            if(n%k == 0){
                int l = 1;
                while ((l <= n - k) && (storage[l%n]==storage[(l+k)%n])){
                    l++;
                }
                if(l > n - k ){
                    result = k;
                    break;
                }
            }
        }
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("OUTPUT.txt"), "UTF-8"));
        out.write(Integer.toString(result));
        out.close();
    }
}