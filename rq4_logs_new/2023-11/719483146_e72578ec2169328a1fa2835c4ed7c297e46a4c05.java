public class Statements {
    public static void main(String[] args) {
        /*
         expression - computes single value
         statement -  stand-alone units of work ( ; in the end makes statement)
         code blocks - a set of zero or more statements, grouped together to achieve a single goal

         */
        int myVariable = 50;
        myVariable++;
        myVariable--;

        System.out.println("This is test");

        System.out.println("This is " +
                "another" +
                "still more");

        /*
         - 3 statements in one line makes difficult for reading, so it is not good practise
         int anotherVariable=50;myVariable++;System.out.println("myVariable"+myVariable);

        - Java ignores spaces;  Whitespaces --> vertical and horizontal whitespaces

        - After using code > Reformat code option in IntelliJ
         */
        int anotherVariable = 50;
        myVariable++;
        System.out.println("myVariable" + myVariable);

        if(myVariable == 0) {
            System.out.println("testagain");
        }

    }
}