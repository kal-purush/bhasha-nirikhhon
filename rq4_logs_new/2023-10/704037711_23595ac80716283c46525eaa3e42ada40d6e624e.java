package O2_DSA_intermediate.O1_27042022_intermediate_dsa_introduction_to_problem_solving;

import java.util.Arrays;

//- There is a circular jail with 100 cells numbered from 1 to 100.
//- Each cell has an inmate and the door is initially locked.
//- One night the jailer gets drunk and he goes around the jail in circles.
//- In ‘ith round’ he goes to every door which is multiple of I and changes the state of the
//door. If the door is open, he will close it and vice versa.
//- He makes a total of 100 rounds, how many prisoners found their door open after 100
//rounds ?

public class Jailer1 {
    private static int getOpenDoorCount(int noOfDoors, int noOfRounds) {
        // Door close - 0, Door open - 1
        noOfDoors++;
        int[] doors = new int[noOfDoors];
        int noOfOpenedDoor = 0;

        for(int i = 1; i <= noOfRounds; i++) { // TC - O(n log n) where n = noOfDoors
            for(int doorNo = i; doorNo < noOfDoors; doorNo = doorNo + i) {
                doors[doorNo] = 1 - doors[doorNo];
            }
        }

        for(int i = 0; i < noOfDoors; i++) { // TC - (n)
            if(doors[i] == 1) {
                noOfOpenedDoor++;
            }
        }

        return noOfOpenedDoor;
    }

    public static void main(String[] args) {
        System.out.println(getOpenDoorCount(100, 100));
         int i[] = new int[10];
         Arrays.fill(i, 10);
         System.out.println(Arrays.toString(i));
    }

}