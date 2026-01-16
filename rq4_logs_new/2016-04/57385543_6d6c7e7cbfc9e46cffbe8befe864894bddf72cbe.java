package dasNeiLianxi3;

/**
 * Created by asus on 2016/4/29.
 */import  java.util.Scanner;
public class SwitchCase {
    public static void  main(String[]  args){
        Scanner  scanner =  new Scanner(System.in);
        System.out.println("inCount: ");
        int count =  scanner.nextInt();
        switch(count){
            case 1:
                System.out.println("student");
                break;
            case 2:
                System.out.println("teacher");
                break;
            case 3:
                System.out.println("schoolmaster");
            default:
                System.out.println("outMan");
        }

    }

}