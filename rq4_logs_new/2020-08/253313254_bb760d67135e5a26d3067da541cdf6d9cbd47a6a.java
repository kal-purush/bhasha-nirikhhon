package J2les2;


import java.util.Arrays;

public class ArrayService {
    public int counter(String[][] strArray) throws MyArrayDataExeption {
        int sum = 0;
        for (int i = 0; i < strArray.length; i++) {
            for (int j = 0; j < strArray[i].length; j++) {
                try {
                    sum += Integer.parseInt(strArray[i][j]);

                } catch (NumberFormatException e) {
                    throw new MyArrayDataExeption("Не верный формат символа" + " "+ i + " " + j + " ");
                }
            }
        }
        return sum;
    }

    public void checkArraySize(Object[][] objects) throws MyArraySizeExeption {
        if (objects.length != 4) {
            throw new MyArraySizeExeption("Не верный размер массива");
        }
        for (int i = 0; i < objects.length; i++) {
            if (objects[i].length != 4) {
                throw new MyArraySizeExeption("Не верный размер массива");
            }
        }
    }
}










