import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Problem1 {

    public static boolean canFollowRule(double length, int n, double min) {

        if ((n - 1) * min <= length) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isFollowingRule(double[] locations, double min) {
        for (int i = 1; i < locations.length; i++) {
            if (locations[i] - locations[i - 1] < min) {
                return false;
            }
        }
        return true;
    }

    public static boolean isFollowingRealRule(double[] locations, boolean[] protection, double min) {
        double locations_unprotected[] = new double[locations.length];
        int num_unprotected = 0;
        for (int i = 0; i < locations.length; i++) {
            if (!protection[i]) {
                locations_unprotected[num_unprotected] = locations[i];
                num_unprotected++;
            }
        }
        for (int i = 0; i < num_unprotected - 1; i++) {
            if ((locations_unprotected[i + 1] - locations_unprotected[i]) < min) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {


        System.out.println(canFollowRule(10.0, 6, 2.0));
        System.out.println(canFollowRule(10.0, 7, 2.0));
        System.out.println(canFollowRule(10.0, 6, 2.5));
        System.out.println(canFollowRule(0.1, 1, 0.0001));

        System.out.println(isFollowingRule(new double[]{0.0, 2.1, 4.5, 10.0}, 2.0));
        System.out.println(isFollowingRule(new double[]{0.0, 1.5, 4.5, 10.0}, 2.0));
        System.out.println(isFollowingRule(new double[]{-10.0, -7.5, 0.0}, 2.4));

        System.out.println(isFollowingRealRule(new double[]{0.0, 1.5, 4.5, 10.0}, new boolean[]{false, true, false, false}, 2.0));
        System.out.println(isFollowingRealRule(new double[]{0.0, 1.5, 1.9, 10.0}, new boolean[]{false, true, false, false}, 2.0));
        System.out.println(isFollowingRealRule(new double[]{0.0, 0.5, 1.0, 1.5, 2.1}, new boolean[]{false, true, true, true, false}, 2.0));


    }
}