package java_basics;

import java.util.Scanner;

public class input {
    public static void main(String[] args) {
      Scanner scanner = new Scanner(System.in);
      System.out.println("Enter you name");
      String name = scanner.nextLine();
      System.out.println("Enter Your age ");
      int Age = scanner.nextInt();
      System.out.println("your name is "+name+ " and Your age is "+Age+"");

      scanner.close();
    }
}