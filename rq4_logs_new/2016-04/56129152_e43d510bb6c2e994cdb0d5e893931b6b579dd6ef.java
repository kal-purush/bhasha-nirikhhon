package main.ua.goit;


import java.util.*;

public class Converter {

//    MAP = Arrays.stream(new Object[][]{
//        {'0', 0}, {'1', 1}, {'2', 2}, {'3', 3}, {'4', 4}, {'5', 5}, {'6', 6}, {'7', 7}, {'8', 8}, {'9', 9}, {'A', 10}, {'B', 11}, {'C', 12}, {'D', 13}, {'E', 14}, {'F', 15}}
//    ).collect(Collectors.toMap(kv -> (Character) kv[0], kv -> (Integer) kv[1]));

    private static final List<Character> VALID_CHARS = Arrays.asList('0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f');

    //MAP - таблица преобразования строкового представления чисел в десятичную систему
    private static final HashMap<Character, Integer> MAP = new HashMap<>();

    private static final HashMap<Integer, Character> MAP_INV = new HashMap<>();

    //Инициализация MAP
    static {
        for (Character sym : VALID_CHARS) MAP.put(sym, VALID_CHARS.indexOf(sym));
        //VALID_CHARS.forEach(sym -> MAP.put(sym, VALID_CHARS.indexOf(sym)) );

        for (Character sym : VALID_CHARS) MAP_INV.put(VALID_CHARS.indexOf(sym), sym);

    }

    // Проверка корректности строки ввода
    public static boolean isValidValue(String param){
        //return Arrays.asList(param).forEach( s -> VALID_CHARS.contains(s));
        return true;
    }

    public static Integer toDecimal(String value, Integer base){

        Integer result = 0;

        List<Integer> arrayInt = new ArrayList<>();

        // Разбиваем строку на символы и заполняем массив Integer значениями из MAP
        for (Character sym: value.toLowerCase().toCharArray()) arrayInt.add(MAP.get(sym));

        //Меняем последовательность элементов массива последний - первый
        Collections.reverse(arrayInt);

        //переводим в десятичное число
        for (Integer dig : arrayInt) result += dig * (int) Math.pow(base, arrayInt.indexOf(dig));

        return result;
    }

    public static String DecimalTo(Integer value, Integer base){

        String result = "";

        List<Character> array = new ArrayList<>();

        while (value != 0) {
            array.add(MAP_INV.get(value % base));
            value /= base;
        }

        Collections.reverse(array);

        for( Character s:array) result+=(s.toString());

        return result;
    }

    //Method converts any value to binary
    //Метод конвертирует любое значение в двоичное
    public static String toBinary(String value, Integer base){
        return DecimalTo(toDecimal(value,base),2);
    }

    //Method converts binary to any value
    //Метод конвертирует двоичное значение в любое число
    public static String fromBinary(String value, Integer base){
        return DecimalTo(toDecimal(value,2),base);
    }


}