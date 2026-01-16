import java.util.*;

public class Decimal {

    // Нужно (было бы неплохо) сделать:
    //      1. Ограничить диапазон ответа если введённое число слишком большое (выводит неправильно значение)
    //      2. Ограничить ввод числами от 0 до 9 и цифрами A B C D E F
    //      3. Добавить минус перед значением если введённое число отрицательное
    //      4.** Добавить вычисление с плавающей точкой

    private Map<Character, Integer> map;

    public Decimal() {
        this.map = new HashMap<>();
        int k = 0;
        for (char j = '0'; j <= '9'; j++) {
            map.put(j, k++);
        }
        for (char i = 'A'; i < 'G'; i++) {
            map.put(i, k++);
        }
    }

    // Перевод любой системы с 2 до 16 в десятичную
    public int ToDecimal(String number, int system) {

        // Разбиваем строку на символы и создаём массив Integer
        List<Integer> arrayInt = new ArrayList<>();
        for (int i = 0; i < number.length(); i++) {
            char symbol = number.charAt(i);
            for (Map.Entry<Character, Integer> c : map.entrySet()) {
                if (symbol == c.getKey()) {
                    arrayInt.add(c.getValue());
                }
            }
        }

        // Переводим систему в десятичную
        Collections.reverse(arrayInt);
        int sum = 0;
        for (int i = 0; i < arrayInt.size(); i++) {
            sum += arrayInt.get(i) * (int) Math.pow(system, i);
        }
        return sum;
    }

    // Перевод из десятичной в любую выбранную систему с 2 до 16
    public String DecimalTo(int number, int system) {

        List<String> array = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        while (number != 0) {
            int remainder = number % system;
            for (Map.Entry<Character, Integer> c : map.entrySet()) {
                if (remainder == c.getValue()) {
                    array.add(String.valueOf(c.getKey()));
                }
            }
            number /= system;
        }

        Collections.reverse(array);
        array.forEach(stringBuilder::append);
        return stringBuilder.toString();
    }
}