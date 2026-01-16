//https://leetcode.com/problems/construct-the-rectangle/
package algorithms.easy;

public class ConstructTheRectangle {
    public static void main(String[] args) {
        ConstructTheRectangle rect = new ConstructTheRectangle();
        System.out.println("Output:\t" + rect.constructRectangle(4));
        System.out.println("Output:\t" + rect.constructRectangle(37));
        System.out.println("Output:\t" + rect.constructRectangleTrialAndError(122122));
    }

    public int[] constructRectangle(int area) {
        int width = (int) Math.sqrt(area);
        while (area % width != 0)
            width--;
        return new int[]{area / width, width};
    }

    public int[] constructRectangleTrialAndError(int area) {
        int length = area;
        int width = 1;
        int[] answer = new int[2];
        while (length >= width) {
            if (area == length * width)
                answer = new int[]{length, width};
            width++;
            length = area / width;
        }
        return answer;
    }
}