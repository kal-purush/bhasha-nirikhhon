import java.util.Arrays;

public class Binary {
    public static void main(String[] args) {

        int[] a = {5, 8, 1, 3, 9 ,6, 22, 43, 10, 12, 99, 193, 20};

        binarySearch(a, 44);

    }

    public static boolean binarySearch(int[] arr, int search){

        Arrays.sort(arr);
        int left = 0;
        int right = arr.length - 1;

        while( left <= right) {
            int mid = left + ((right - left) / 2);
            if(arr[mid] == search) {
                System.out.println(search + " was found");
                return true;
            }
            else if(search < arr[mid]){
                right = mid -1;
            }
            else {
                left = mid + 1;
            }
        }

        System.out.println(search + " was NOT found");

        return false;
    }
}