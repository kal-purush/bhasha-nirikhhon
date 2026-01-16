public class PositiveNegativeZero {

    public static void checkNumber(int number){

        if(number > 0){
            System.out.println("positive");
        } else if (number < 0) {
            System.out.println("negative");
        } else {
            System.out.println("zero");
        }

    }

    public static void main(String[] args) {

        int number = 12;
        checkNumber(number);

        number =-12;
        checkNumber(number);

        number=0;
        checkNumber(number);

    }
}