package lesson2;

public class Main2 {


    public static int sumOfElements (String [][]arr) throws MyArrayDataException, MyArraySizeException {
        int size = 4;
        int sum = 0;
        try {
            if (size != 4) {
                throw new MyArraySizeException("Неверный размер массива");
            } else {
                for (int i = 0; i <arr.length ; i++) {
                    for (int j = 0; j <arr.length ; j++) {
                        try {
                            sum += Integer.parseInt(arr[i][j]);
                            throw new MyArrayDataException("Неверный формат данных", i,j);
                        } catch (MyArrayDataException ex) {
                            ex.getMessage();
                        }
                    }
                }
            }
        } catch (MyArraySizeException ex) {
            System.out.println(ex.getMessage());
        }
        return sum;

}

    public static void main(String[] args) {
        String [][] arr = {{"1","2","3","4"},{"1","2","3","4"},{"1","2","3","4"}, {"1","2","3","4"}};
        try {
            System.out.println(sumOfElements(arr));
        }   catch (MyArraySizeException | MyArrayDataException ex) {
            ex.getMessage();
        }





    }

}