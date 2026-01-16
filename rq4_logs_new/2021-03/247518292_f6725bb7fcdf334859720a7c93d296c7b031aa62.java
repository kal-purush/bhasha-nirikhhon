
import java.util.Arrays;

// Task
// King Arthur and his knights are having a New Years party. Last year Lancelot was jealous of Arthur, because Arthur had a date and Lancelot did not, and they started a duel.

// To prevent this from happening again, Arthur wants to make sure that there are at least as many women as men at this year's party. He gave you a list of integers of all the party goers.

// Arthur needs you to return true if he needs to invite more women or false if he is all set.

// -1 Women
// 1 Man
class InviteMoreWoman {

    // v1
    public static boolean inviteMoreWomenv2(int[] l) {
        int n = 0;
        for (int i = 0; i < l.length; i++) {
            n += l[i];
        }
        return n > 0;
    }

    public static boolean inviteMoreWomen(int[] l) {
        return Arrays.stream(l).sum() > 0;
    }

    public static boolean assertEquals(boolean hopeResult, boolean test) {
        System.out.println(hopeResult == test);
        return hopeResult == test;
    }

    public static void main(String[] args) {
        assertEquals(true, inviteMoreWomen(new int[] { 1, -1, 1 }));
        assertEquals(false, inviteMoreWomen(new int[] { -1, -1, -1 }));
        assertEquals(false, inviteMoreWomen(new int[] { 1, -1 }));
        assertEquals(true, inviteMoreWomen(new int[] { 1, 1, 1 }));
    }

}