package src.main.java.array;

public class TrapRainWater {
    public static int trap(int[] height) {
        //Start with 2 pointers both at 0
        //at each step create a temp counter and add it to the main counter after finding  a bigger right bound
        // if you find a bigger one move left to curr
        // else return right to left-bound and keep moving.

        int left = 0;
        int right = 0;
        int count = 0;
        int tempCount = 0;

        if (height.length == 1) {
            return 0;
        }

        while (right < height.length - 1) {

            //move right pointer till you meet a height bigger than left bound
            while (left <= right) {
                right += 1;
                if (right < height.length - 1) {
                    tempCount += height[left] - height[right];
                } else {
                    break;
                }

                if (height[right] > height[left] && tempCount>0) {
                    count += tempCount;
                    tempCount = 0;
                    left = right;
                } else {
                    left += 1;
                    right = left + 1;
                }
            }
        }

        return count;
    }

    public static void main(String[] args){
        int[] A = {0,1,0,2,1,0,1,3,2,1,2,1};
        //int[] A = {4,2,0,3,2,5};

        System.out.println(trap(A));
    }
}