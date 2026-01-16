package kruzuzdyak;

public class MathOperationsDemo {

    public static void main(String[] args) {
        System.out.println("Welcome to the 'Math Operations\n");

        int a = 10;
        int b = 31;
        int sum = a + b;

        System.out.printf("Addition '+':\na = %d, b = %d\n", a, b);
        System.out.printf("a + b = %d + %d = %d\n\n", a, b, sum);

        long x = 23L;
        long y = 46L;
        long sub = x - y;
        System.out.printf("Subtraction '-':\nx = %d, y = %d\n", x, y);
        System.out.printf("x - y = %d - %d = %d\n\n", x, y, sub);

        int c = 42;
        int d = 12;
        int multiply = c * d;
        System.out.printf("Multiplication '*':\nc = %d, d = %d\n", c, d);
        System.out.printf("c * d = %d * %d = %d\n\n", c, d, multiply);

        short m = 20;
        short n = 2;
        System.out.printf("Division '/':\nm = %d, n = %d\n", m, n);
        System.out.printf("m / n = %d / %d = %d\n\n", m, n, m / n);

        int e = 42;
        int f = 10;
        int remainder = e % f;
        System.out.printf("Remainder '%%':\ne = %d, f = %d\n", e, f);
        System.out.printf("e %% f = %d %% %d = %d\n\n", e, f, remainder);
    }
}