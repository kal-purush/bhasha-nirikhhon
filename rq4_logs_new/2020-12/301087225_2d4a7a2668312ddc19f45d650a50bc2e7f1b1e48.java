import java.util.Scanner;

public class Nomor1
{
    public static void main(String[] args)
    {
        Scanner scan = new Scanner(System.in);
        int inp1 = scan.nextInt();
        int inp2 = scan.nextInt();

        for(int i = inp1>inp2?inp2:inp1;i<=(inp1>inp2?inp1:inp2);i++)
        {
            if(i%2==0)
            {
                if(i==0)
                    System.out.println(i+" nol");
                else if(i<0)
                    System.out.println(i+" genap negatif");
                else
                    System.out.println(i+" genap positif");
            }
            if(i%2!=0)
            {
                if(i==0)
                    System.out.println(i+" nol");
                else if(i<0)
                    System.out.println(i+" ganjil negatif");
                else
                    System.out.println(i+" ganjil positif");
            }
        }
    }
}