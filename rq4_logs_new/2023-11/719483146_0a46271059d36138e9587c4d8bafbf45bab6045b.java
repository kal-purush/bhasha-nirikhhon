public class CodeBlocks {
    public static void main(String[] args) {
        /*
         expression - computes single value
         statement -  stand-alone units of work ( ; in the end makes statement)
         code blocks - a set of zero or more statements, grouped together to achieve a single goal
            eg: if ,if else
         */
        boolean gameOver = true;
        int score = 5000;
        int levelCompleted = 5;
        int bonus = 100;
        // If code block
        if (score == 5000) {
            System.out.println("Your score was 5000");
        }
        //if - else code block
        if (score < 5000) {
            System.out.println("Your score is less than 5000");
        } else {
            System.out.println("Your score is greater than 5000");
        }
        // if - else if code block
        if (score < 5000 && score > 1000) {
            System.out.println("Your score is less than 5000 but grater than 1000");
        } else if (score < 1000) {
            System.out.println("Your score is less than 1000");
        } else {
            System.out.println("Your score is greater than 5000");
        }
    }
}