package training;
public class binaryTreeSearch {

    public static void main(String[] args) {

        int[] nums = {1, 3, 4, 5, 7, 9, 11};
        System.out.println(binarySearch(nums, 7));

    }


    private static int binarySearch(int[] numbers, int numberToFind) {
        int low = 0;
        int high = numbers.length - 1;

        while (low <= high) {
            int middlePossition = (low + high) / 2;
            int middleNumber = numbers[middlePossition];

            if (middleNumber == numberToFind) {
                return middlePossition;
            }
            if (middleNumber > numberToFind) {
                high = middlePossition - 1;
            } else {
                low = middlePossition + 1;
            }
        }

        return -1;
    }
}