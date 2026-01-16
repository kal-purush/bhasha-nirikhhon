import java.util.*;
public class Lcm{
    
    static int lcm(int a, int b)
    {
        int g = 1;
        for(int i = 1; i<=a; i++)
        {
            if(a%i == 0 && b%i == 0)
            {
                g = i;
            }
        }
        return (a*b)/g;
    }
     public static void main(String []args){
        Scanner sc = new Scanner(System.in);
        
        int n1 = sc.nextInt();
        int n2 = sc.nextInt();
        
        System.out.print(lcm(n1,n2));
     }
}