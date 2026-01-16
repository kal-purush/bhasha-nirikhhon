import java.util.Scanner;

public class EveOdd {
    public static  void main(String me[]){
        EveOdd ev=new EveOdd();
        Scanner sc=new Scanner(System.in);
        System.out.println("Enter a Positive Number");
        int n=sc.nextInt();
        boolean s;
       s= ev.chk(n);
       if(s){
           System.out.println("Odd");
       }
       else{
           System.out.println("Even");
       }
    }

//     void chk(int x){
//        if(x%2!=0){
//            System.out.println("Odd");
//        }
//        else{
//            System.out.println("Even");
//        }
//     }
boolean chk(int x){
    if(x%2!=0){

        return true;
    }
    else{

        return false;
    }
}
}