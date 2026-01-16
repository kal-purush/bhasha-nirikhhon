package lesson4.task4_3;

public class MyEquals<X, Y> {
    X num1;
    Y num2;

    public MyEquals(X o1, Y o2) {

        num1 = o1;
        num2 = o2;
    }

    void getMax() {
        if (num1 instanceof Integer && num2 instanceof Integer) {
            if (((Integer) num1).compareTo((Integer) num2) > 0) {
                System.out.printf("Первое знаечение %s больше второго %s \n", num1, num2);
            } else {
                System.out.printf("Второе значение %s больше первого %s \n", num2, num1);
            }
        } else if (num1 instanceof String && num2 instanceof String) {
            if (((String) num1).compareTo((String) num2) > 0) {
                System.out.printf("Первое знаечение %s больше второго %s \n", num1, num2);
            } else {
                System.out.printf("Второе значение %s больше первого %s \n", num2, num1);
            }
        } else {
            if (num1 instanceof Double && num2 instanceof Double) {
                if (((Double) num1).compareTo((Double) num2) > 0) {
                    System.out.printf("Первое знаечение %s больше второго %s \n", num1, num2);
                } else {
                    System.out.printf("Второе значение %s больше первого %s \n", num2, num1);
                }
            }
        }
    }


    public X getNum1() {
        return num1;
    }

    public Y getNum2() {
        return num2;
    }
}