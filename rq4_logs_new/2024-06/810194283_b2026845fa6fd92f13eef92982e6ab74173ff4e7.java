import java.util.Scanner;

class subject{
    int roll_no,sub;
    char score;
    String name;
    
}

class Grade{
    subject  s1 = new subject();


    char checkGrade(double percent){
        char x;
        if(percent>90)
            x = 'O';
        else if(percent>70 && percent <90)
            x = 'A';
        else if(percent>50 && percent<70)
            x = 'B';
        else if(percent>40 && percent<50)
            x = 'C';
        else 
            x = 'F';
        
        return x;
    }
}

public class student {
    public static void main(String[] args) {
        int total = 0;
        Scanner sc = new Scanner(System.in);
        subject s1 = new subject();
        Grade g1 = new Grade();

        System.out.println("\nStudennt Grade Calculator  ");

        System.out.print("\nEnter student roll number   :- ");
        s1.roll_no = sc.nextInt();

        System.out.print("Enter Student Name   :- ");
        s1.name = sc.next();

        System.out.print("\nEnter total subjects ");
        s1.sub = sc.nextInt();
        System.out.println();
        for(int i = 1;i<=s1.sub;i++){
            System.out.print("Enter marks for "+i + " subject outoff 100 :-  ");
            int sub = sc.nextInt();
            total +=sub;
        }
        double Percent = (double) total / s1.sub;
        char x = g1.checkGrade(Percent);
        System.out.println("\nTotal Marks Obtained by "+s1.name+" are :- " +total);
        System.out.println("Percentage Obtained by "+s1.name+" are :- "+Percent);
        System.out.println("Grade Obtained by "+s1.name+" is :- "+x);
    }
}