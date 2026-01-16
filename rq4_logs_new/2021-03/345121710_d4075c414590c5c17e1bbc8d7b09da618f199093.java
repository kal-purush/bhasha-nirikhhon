public class ArraysWork implements IArraysWork {

    @Override
    public int[] extractArray(int[] input) {
        if (input.length == 0) {
            throw new RuntimeException();
        }
        int idx = -1;
        for (int i = 0; i < input.length; i++) {
            if (input[i] == 4) {
                idx = i + 1;
            }
        }
        if (idx == -1) {
            throw new RuntimeException();
        }
        int l = input.length - idx;
        int[] result = new int[l];
        System.arraycopy(input, idx,result,0, l);
        return result;
    }

    @Override
    public boolean checkArray(int[] input) {
        boolean hasOne = false;
        boolean hasFour = false;
        for (int item: input) {
            if (item == 1) {
                hasOne = true;
            } else if (item == 4) {
                hasFour = true;
            } else {
                return false;
            }
        }
        return hasOne && hasFour;
    }

}