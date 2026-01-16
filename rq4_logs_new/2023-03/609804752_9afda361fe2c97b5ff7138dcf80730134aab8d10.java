
package basic.anisulvideo2;

public class MathDemo {
    public static void main(String[] args)
    {
        int a=20;
        int b=10;
        int x=2,y=3;
        int max=Math.max(a, b);
        System.out.println("Maximum number is "+max);
        int min = Math.min(a, b);
        System.out.println("Minimum = "+min);
        
        int absolute = Math.abs( b);
        System.out.println("absolute = "+absolute);
        
        double power = Math.pow(x, y);
        System.out.println("a to the power b is = "+power);
        
        int round=Math.round(8.6f);
        System.out.println("Round = "+round);
        
        double pi=Math.PI;
        System.out.println("pi="+pi);  
    }
}