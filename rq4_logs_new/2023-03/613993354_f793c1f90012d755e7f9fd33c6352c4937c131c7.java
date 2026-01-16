package by.ekids;

public class ArrayExample {
    public static void main(String[] args) {
        int a = 10;
        //9, 8, 4, 5 ...
        //массивы
        int[] averageMarks = {1, 2, 4, 6, 8};
        //averageMarks = {1, 2, 4, 6, 8}
        //2 способ
        int[] averageMarks2;
        averageMarks2 = new int[10];

        int[] averageMarks3 = new int[7];

        int[][] averageMarksForSchool = {{1, 2, 4, 6, 8},
                                        {1,10, 4, 6, 8},
                                        {6, 2, 4, 6, 8}};
        int[][] averageMarksForSchool2 = new int[6][5];


        for (int i = 0; i < averageMarks.length; i++) {
            System.out.println(i + ": " + averageMarks[i]);
        }

        for (int i = 0; i < averageMarksForSchool[1].length; i++) {

            System.out.println();

            for (int j = 0; j < averageMarksForSchool[i].length; j++) {

                System.out.print(averageMarksForSchool[i][j] + " ");

            }
        }
    }
}