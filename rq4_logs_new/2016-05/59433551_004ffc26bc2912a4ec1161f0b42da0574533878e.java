import java.util.Scanner;

public class ADifferentProblem {

  public static void main(String[] args) {

    Scanner sc = new Scanner(System.in);

    while(sc.hasNext()) {
      long a = sc.nextLong();
      long b = sc.nextLong();

      System.out.println(Math.abs(b-a));
    }

  }
}