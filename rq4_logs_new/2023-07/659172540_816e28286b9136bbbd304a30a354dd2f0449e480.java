package learningMethodAndClasess;

import java.util.Stack;

/**
 * В Java, "стек" (stack) - это структура данных, которая представляет собой упорядоченный список элементов,
 * где добавление новых элементов и удаление существующих происходит только с одного конца,
 * который называется вершиной стека (top of the stack).
 * Этот принцип называется "Last In, First Out" (LIFO),
 * что означает, что последний элемент, добавленный в стек, будет первым,
 * который будет извлечен из стека.
 */
public class StackClass {
    public static void main(String[] args) {
        Stack<String> stack = new Stack<>();
        stack.push("Element three");
        stack.push("Element two");
        stack.push("Element one");
        System.out.println( " Stack " + stack);

        String topElement = stack.peek(); //возвращает значение элемента с вершины стека, но не удаляет его
        System.out.println(topElement + " Метод peek возвращает элемент из топа стека " + topElement);

        int stackSize = stack.size(); //возвращает размер стека
        System.out.println(stackSize + " размер стека ");

        Boolean empty = stack.isEmpty();  // переменная типа boolean возрвщает true или false
        System.out.println("Проверка стека на пустоту " + empty);
    }

}