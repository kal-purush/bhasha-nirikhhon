public class ArrayTraversalP7 {
    public static void main(String[] args) {
        int[] array = {5,2,4,1,3};
        int[] reverse = array;
        gogogadget(array, reverse);
        System.out.print(reverse);

    }
    public static void gogogadget(int[] og, int[] reverse){

        int length = og.length;
        for (int i = 1; i<=length; i++) {
            int cur = og[length-i];
            reverse[i] = cur;
        }
    }

}