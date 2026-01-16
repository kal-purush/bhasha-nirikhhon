public class ifelse14 {
    public static void main(String[] args) {

        int num = 3553, rev = 0, rem;
        int orig = num;
        while (num != 0) {
            rem = num % 10;
            rev = rev * 10 + rem;
            num /= 10;
        }


        if (orig == rev) {
            System.out.println(orig + " is Palindrome.");
        }
        else {
            System.out.println(orig + " is not Palindrome.");
        }
    }
}