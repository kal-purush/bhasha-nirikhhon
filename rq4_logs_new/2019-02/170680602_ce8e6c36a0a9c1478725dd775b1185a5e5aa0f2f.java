package lesson2;

public class MyExceptions {
    // 1. Напишите метод, на вход которого подаётся двумерный строковый массив размером 4х4,
    // при подаче массива другого размера необходимо бросить исключение MyArraySizeException.
    public static int SumArray (String[][] matr) throws MyArraySizeException, MyArrayDataException{
        int sum = 0;

        if (matr.length != 4) throw new MyArraySizeException();
        for (int i = 0; i < 4; i++) {
            if (matr[i].length != 4) throw new MyArraySizeException();
        }
        // 2. Далее метод должен пройтись по всем элементам массива, преобразовать в int, и просуммировать.
        // Если в каком-то элементе массива преобразование не удалось (например, в ячейке лежит символ
        // или текст вместо числа), должно быть брошено исключение MyArrayDataException,
        // с детализацией в какой именно ячейке лежат неверные данные.
        for (int i = 0; i < matr.length; i++) {
            for (int j = 0; j < matr[i].length; j++) {
                try {
                    sum += Integer.parseInt(matr[i][j]);
                }
                catch (NumberFormatException e) {
                    throw new MyArrayDataException(i,j);
                }
            }
        }
        return sum;
    }

    public static void main(String[] args) {
        String[][] matr = {{"1", "2", "3", "4"}, {"5", "6", "7", "8"}, {"9", "10", "11", "12"}, {"13", "14", "15", "16"}};
        // 3. В методе main() вызвать полученный метод, обработать возможные исключения
        // MySizeArrayException и MyArrayDataException, и вывести результат расчета.
        try {
            System.out.println("Сумма элементов массива равна "+SumArray(matr));
        }
        catch (MyArraySizeException e) {
            System.out.println("Массив неправильного размера");
        }
        catch (MyArrayDataException e) {
            System.out.println("В ячейке ("+e.line+":"+e.column+") не целое число");
        }
        finally {
            System.out.println("Работа программы завершена");
        }
    }
}