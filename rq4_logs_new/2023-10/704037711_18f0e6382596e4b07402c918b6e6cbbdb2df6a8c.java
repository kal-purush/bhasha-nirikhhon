package O2_DSA_intermediate.O3_02052022_intermediate_dsa_time_complexity_2;

/*
This recursion function helps in understanding recursion function's execution effectively.
To understand it properly we should try to use some break points and debug it.
*/
public class Recusion {
    private static void stepsToHome(int stepNo, int totalStepsNeeded) {

        // base case
        if(stepNo == totalStepsNeeded) {
            System.out.println("Reached home");
            return;
        }

        // recursive case
        System.out.println("Taking step no.: " + stepNo);
        stepsToHome(stepNo+1, totalStepsNeeded);
        System.out.println("After we reached home");
    }
    public static void main(String[] args) {
        stepsToHome(0, 10);
    }
}