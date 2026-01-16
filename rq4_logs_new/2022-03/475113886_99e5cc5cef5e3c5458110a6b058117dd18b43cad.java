public class ActualCode {
    public static void main(String[] args) {
        int i,fact=1;
        int n=5;
        for(i=1;i<=n;i++){
            System.out.println("i value is "+i);
            fact=fact*i;
            System.out.println("fact value is "+fact);
        }
        System.out.println("Factorial of "+n+" is: "+fact);
    }
}