/*
* 1. Напишите метод, на вход которого подается двумерный строковый массив размером 4х4, при подаче массива
* другого размера необходимо бросить исключение MyArraySizeException.
2. Далее метод должен пройтись по всем элементам массива, преобразовать в int, и просуммировать.
* Если в каком-то элементе массива преобразование не удалось (например, в ячейке лежит символ
* или текст вместо числа), должно быть брошено исключение MyArrayDataException – с детализацией,
*  в какой именно ячейке лежат неверные данные.
3. В методе main() вызвать полученный метод, обработать возможные исключения MySizeArrayException
* и MyArrayDataException и вывести результат расчета.
* */

package exception;

public class Main {
    public static void main(String[] args) {

        String [][] massiv = {
                {"2", "3", "k4"},
                {"21", "31", "51"},
                {"212", "312", "512"}
        };

        try {
            // Метод проверяет на соответствеие длины массива
            check_massiv(massiv, massiv.length);

            // Метод check_element_massiv проверяет элементы массива
            System.out.printf("итого сумма массива: " + check_element_massiv(massiv));

        } catch (MyArraySizeException e) {
            e.printStackTrace();
            System.out.printf(" " + e.getLong_ms());
        } catch (MyArrayDataException e) {
            e.printStackTrace();
            System.out.printf("Ошибка в элементе: " + e.getElement() + " в ячейке: " + e.getI() + "/" + e.getJ());
        }
    }

    // Метод проверяет на соответствеие длины массива
    public static void check_massiv(String arr [][], int long_ms) throws MyArraySizeException {

        if (arr.length != long_ms){
            throw new MyArraySizeException("Некорректный массив ", arr.length);
        }

        for (int ii = 0; ii < arr.length; ii++) {
            if (arr[ii].length != long_ms){
                throw new MyArraySizeException("Ошибка. В массиве есть некорректная длина массива " , arr.length);
            }
        }
    }

    // Метод проверяет элементы массива
    public static int check_element_massiv(String arr[][]) throws MyArrayDataException {

        int summa = 0;

        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[i].length; j++) {


                try {
                    summa = summa + Integer.parseInt(arr[i][j]);
                } catch(NumberFormatException e) {
                    throw new MyArrayDataException("Не могу преобразовать элемент: ", arr[i][j], i, j);
                };

            }
        }

       return summa;
    }
}