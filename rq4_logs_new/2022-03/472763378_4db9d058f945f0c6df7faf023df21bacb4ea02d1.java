public class BitArray {
    static int cellSize = 3;
    public static void main(String[] args) {
        long[] longArray = new long[10];
//        long oneCell = 0b0010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010_1010L;

        int twoMarks = 0b101_101_101;//45     5 5 5
        int threeMarks = 0b000_100_110_101;
        int mark2 = (threeMarks << 26) >> 29;
        System.out.println(mark2);

        int fourMark = 0b101_100_011_010_001;

//        int mark1 = (threeMarks >> 3);
//        mark1 = mark1 << 3;
//        mark1 = threeMarks - mark1;

//        System.out.println(Integer.toBinaryString((threeMarks << (31 - (2 * 3))) >> (31 - 3)));
          getMark(fourMark,4);
    }
    public static void getMark(int markArray, int markNumber) {
//        int mark =
//        System.out.printf("mark number %d is %s", markNumber, Integer.toBinaryString(mark));
    }

}