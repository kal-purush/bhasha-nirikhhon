package Lesson2Except;

public class MainClass {

    public static void main(String[] args){
        String[][] myArr = {{"1", "2", "3", "4"}, {"5", "6", "7", "8"}, {"9", "0", "9", "8"}, {"7", "6", "5", "щ"}};
        for(String[] lineArr : myArr){
            for(String line : lineArr){
                System.out.print(line + '\t');
            }
            System.out.println();
        }

        try {
            System.out.println("The sum of the array elements is " + getSumArray(myArray(myArr)));
        }
        catch (MyArraySizeException e){
            System.out.println(e.getMessage());
        }
        catch (MyArrayDataException e){
            System.out.println(e.getMessage());
        }

    }

    private static String[][] myArray(String[][] myArr){
        if (myArr.length != 4 ) {
                throw new MyArraySizeException();
            }
        for (String[] strings : myArr)
            if (strings.length != 4) {
                throw new MyArraySizeException();
            }

        return myArr;
    }

    private static int getSumArray(String[][] myArr) {
        int sum = 0;

        for (int i = 0; i < myArr.length; i++)
            for (int j = 0; j < myArr[0].length; j++) {
//                try{
                    sum += Integer.parseInt(inputDate(myArr[i][j], i, j));
//                }
//                catch (MyArrayDataException e){
//                    System.out.println(e.getMessage());  Не до конца понял последнее задание, поэтому добавил блок,
//                }                                        который рассчитывает ссумму элементов массива, даже при
            }                                            //исключении MyArrayDataException, хотя подозреваю,
        return sum;                                      //что это не нужно.
    }

    private static String inputDate(String s, int x, int y) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                throw new MyArrayDataException(x, y);
            }
        }
        return s;
    }
}