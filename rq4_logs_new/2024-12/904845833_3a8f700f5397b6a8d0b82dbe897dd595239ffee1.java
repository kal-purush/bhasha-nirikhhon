class ArithmeticExpression {
    private String operand1; // Первый операнд
    private String operand2; // Второй операнд
    private String operator; // Оператор (+, -, *, /)

    /**
     * Конструктор, принимающий строковое выражение для разбора.
     *
     * @param input Строка вида "операнд1 оператор операнд2".
     */
    public ArithmeticExpression(String input) {
        setExpression(input);
    }

    /**
     * Конструктор клонирования.
     *
     * @param other Другой объект ArithmeticExpression для копирования.
     */
    public ArithmeticExpression(ArithmeticExpression other) {
        this.operand1 = other.operand1;
        this.operator = other.operator;
        this.operand2 = other.operand2;
    }

    /**
     * Метод для разбора выражения (разделение строки на операнды и оператор).
     *
     * @param input Строка вида "операнд1 оператор операнд2".
     */
    private void setExpression(String input) {
        String[] parts = input.split(" ");
        if (parts.length != 3) {
            System.out.println("Ошибка: Неверный формат выражения! Ожидается формат: операнд1 оператор операнд2.");
            return;
        }

        this.operand1 = parts[0];
        this.operator = parts[1];
        this.operand2 = parts[2];

        // Проверка на пустые операнды или оператор
        if (operand1.isEmpty() || operand2.isEmpty() || operator.isEmpty()) {
            System.out.println("Ошибка: Операнды и оператор не могут быть пустыми!");
            return;
        }

        // Проверка на корректность операндов
        try {
            Double.parseDouble(operand1);
        } catch (NumberFormatException e) {
            System.out.println("Ошибка: Первый операнд не является числом!");
            return;
        }
        try {
            Double.parseDouble(operand2);
        } catch (NumberFormatException e) {
            System.out.println("Ошибка: Второй операнд не является числом!");
            return;
        }

        // Проверка на допустимые операторы
        if (!operator.equals("+") && !operator.equals("-") && !operator.equals("*") && !operator.equals("/")) {
            System.out.println("Ошибка: Неизвестный оператор! Допустимые операторы: +, -, *, /.");
            return;
        }
    }

    /**
     * Метод для вычисления результата выражения.
     *
     * @return Результат вычисления.
     */
    public double calculate() {
        // Проверка, что выражение корректно
        if (operand1 == null || operand2 == null || operator == null) {
            System.out.println("Ошибка: Выражение не было правильно разобрано!");
            return 0;
        }

        double num1 = Double.parseDouble(operand1); // Преобразование первого операнда в число
        double num2 = Double.parseDouble(operand2); // Преобразование второго операнда в число

        // Выбор операции по оператору
        switch (operator) {
            case "+":
                return num1 + num2; // Сложение
            case "-":
                return num1 - num2; // Вычитание
            case "*":
                return num1 * num2; // Умножение
            case "/":
                if (num2 == 0) {
                    System.out.println("Ошибка: Деление на ноль!");
                    return 0;
                }
                return num1 / num2; // Деление
            default:
                System.out.println("Ошибка: Неизвестный оператор!");
                return 0;
        }
    }
}