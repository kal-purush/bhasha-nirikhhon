import java.util.Scanner;

public class AppMain {
    public static void main(String[] args) {
        Calculator calculator = new Calculator();
        Scanner sc = new Scanner(System.in);

        /* 반복문 시작 */
        while (true) {
            System.out.print("첫 번째 숫자를 입력하세요: ");
            int num1 = sc.nextInt();
            System.out.print("두 번째 숫자를 입력하세요: ");
            int num2 = sc.nextInt();

            System.out.print("사칙연산 기호를 입력하세요: ");
            char operator = sc.next().charAt(0);

            double result = calculator.calculate(num1, num2, operator);
            calculator.setResult(result);
            System.out.println("result = " + result);

            System.out.println("더 계산하시겠습니까? (exit 입력 시 종료)");
            String comment = sc.next();
            if (comment.equals("exit")) {
                break;
            }
        }
        /* 반복문 종료 */
    }
}