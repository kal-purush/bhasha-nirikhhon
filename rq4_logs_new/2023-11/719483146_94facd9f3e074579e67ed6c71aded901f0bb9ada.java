public class Method {
    public static void main(String[] args) {

        boolean gameOver = true;
        int score = 5000;
        int levelCompleted = 5;
        int bonus = 100;

        System.out.println("The next high score is " +
                calculateScore(gameOver,score,levelCompleted,bonus));

        score = 8000;
        levelCompleted = 8;
        bonus = 300;

        int finalScore =  calculateScore(gameOver,score,levelCompleted,bonus);
        System.out.println("The next high score is " + finalScore);
    }

    public static int calculateScore(boolean gameOver, int score, int levelCompleted, int bonus){
        int finalScore = score;

        if (gameOver == true) {
            finalScore += (levelCompleted * bonus);
        }

        return finalScore;

    }



}