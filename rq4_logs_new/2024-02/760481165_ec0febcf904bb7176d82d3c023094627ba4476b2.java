import java.util.Scanner;

public class StringCalculator {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String operand1 = "";
        String operator = "";
        String operand2 = "";
        try {
            System.out.println("Введите выражение в формате: I + II, 100 + 500, Hello World - World, Example!!!/3, Java * 5: ");
            String input = scanner.nextLine();
            // "Hi World!" - "World!"
            String[] parts = input.split(" ");
            if (parts.length >= 5) {
                throw new IllegalArgumentException("Неверный формат ввода");
            }
            if (parts.length == 4) {
                String half = readOperand(parts[0]);
                String twoHalf = readOperand(parts[1]);
                operand1 = half + twoHalf;
                operator = parts[2];
                operand2 = readOperand(parts[3]);
            }
            if (parts.length == 3) {
                operand1 = readOperand(parts[0]);
                operator = parts[1];
                operand2 = readOperand(parts[2]);
            }

            if (!isValidOperator(operator)) {
                throw new IllegalArgumentException("Неподдерживаемая операция");
            }

            String result;
            try {
                int num = Integer.parseInt(operand2);
                result = calculate(operand1, operator, num);
            } catch (NumberFormatException e) {
                result = calculate(operand1, operator, operand2);
            }

            System.out.println("Результат: \"" + result + "\"");
        } catch (Exception e) {
            System.out.println("Ошибка: " + e.getMessage());
        }
    }

    public static boolean isValidOperator(String operator) {
        return operator.equals("+") ||  operator.equals("-") || operator.equals("*") || operator.equals("/");
    }

    public static String calculate(String operand1, String operator, int operand2) {
        switch (operator) {
            case "+":
                return operand1 + operand2;
            case "-":
                if (operand1.contains(String.valueOf(operand2))) {
                    return operand1.replace(String.valueOf(operand2), "");
                } else {
                    return operand1;
                }
            case "*":
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < operand2; i++) {
                    sb.append(operand1);
                }
                return sb.toString();
            case "/":
                int newLength = operand1.length() / operand2;
                return operand1.substring(0, newLength);
            default:
                throw new IllegalArgumentException("Неподдерживаемая операция");
        }
    }

    public static String calculate(String operand1, String operator, String operand2) {
        switch (operator) {
            case "+":
                return operand1 + operand2;
            case "-":
                if (operand1.contains(operand2)) {
                    return operand1.replace(operand2, "");
                } else {
                    return operand1;
                }
            default:
                throw new IllegalArgumentException("Неподдерживаемая операция");
        }
    }

    public static String readOperand(String operand) {
        int length = operand.length();
        if (length >= 2 && operand.charAt(0) == '"' && operand.charAt(length - 1) == '"') {
            return operand.substring(1, length - 1);
        }
        return operand;
    }

}