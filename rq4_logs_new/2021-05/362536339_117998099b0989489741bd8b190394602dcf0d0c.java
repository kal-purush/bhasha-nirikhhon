import java.util.Scanner;

class Solution {

  public int addDigits(int num) {

    int calc = 0;
    int calc2 = 0;
    int calc3 = 0;

    while (num != 0) {
      int b = num % 10;
      num = num / 10;
      calc = b + calc;
    }
    while (calc != 0) {
      int x = calc % 10;

      calc = calc / 10;
      calc2 = x + calc2;

    }
    if (calc2 >= 10) {
      while (calc2 != 0) {
        int m = calc2 % 10;

        calc2 = calc2 / 10;
        calc3 = m + calc3;
        System.out.println(calc3);

      }
      return calc3;

    } else {
      System.out.println(calc2);
      return calc2;

    }

  }
}

class add {

  public static void main(String[] args) {
    Scanner insert = new Scanner(System.in);
    System.out.println("enter an integer");

    try {
      int num = insert.nextInt();

      Solution a = new Solution();
      a.addDigits(num);
    } finally {
      insert.close();
    }

  }
}