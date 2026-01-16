package stepikCourse.Java.generics;

public class GenericClass {
    public static void main(String[] args) {
        /**
         * при инициализации дженерика можем задать любой тип переменной и использовать
         */
        Info<Integer> int1 = new Info<>(34); //integer
        System.out.println("Наша цифра " + int1);
        Integer in = int1.getValue();
        System.out.println("Значение переменной " + in);

        Info<String> string1 = new Info<>("Nick"); //string
        System.out.println("Наше имя " + string1);
        String str = string1.getValue();
        System.out.println("Значение переменной " + str);

    }
}

//параметризованные методы
class Info<K> {
    private K value;

    public Info(K value) { // в конструктор передали переменную T
        this.value = value;
    }

    public String toString() { //создали метод toString
        return "" + value + "";
    }

    public K getValue() {
        return value;
    }


}

