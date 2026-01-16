import java.util.Scanner;
public class creditcard {
    public static void main(String args[]){
        Scanner sc = new Scanner(System.in);
        long cardNo = sc.nextLong();
        sc.close();
        int sum = 0;
        while(cardNo > 0){
            sum += cardNo % 10;
            cardNo /= 10;
            int next = (int)(cardNo % 10) * 2;
            sum = next > 9 ? sum + next - 9 : sum + next;
            cardNo /= 10;
        }
        System.out.println(sum % 10 == 0 ? "VALID" : "INVALID");
    }
}
