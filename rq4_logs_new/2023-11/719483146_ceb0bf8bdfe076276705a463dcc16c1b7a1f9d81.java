public class Expressions {

    public static void main(String[] args){
        /*
         expression - computes single value
         statement -  stand-alone units of work
         code blocks - a set of zero or more statements, grouped together to achieve a single goal

         */

        double kilometere = (100 *1.609344);

        int highScore = 50;
        if (highScore > 25){
            highScore = 1000 + highScore;
        }

        /*
         6 expressions in below code ;
        1. health = 100
        2. health < 25
        3. highScore >100
        4. (health < 25) && (highScore >100)
        5. highScore -100
        6. highScore = highScore -100
         */

        int health = 100;
        if((health < 25) && (highScore >100)){
            highScore = highScore -100;
        }
    }

}