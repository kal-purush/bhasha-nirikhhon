package PE_01_to_16;

import java.util.ArrayList;
import java.util.Collections;

public class PE_07 {
    private String problem_title = "10001st Prime Number";
    private int problem_number = 07;
    private boolean completed_successfully = true;

    public PE_07(){
        System.out.println("Starting Project Euler Problem No. "+ problem_number +": "+problem_title);
        System.out.println("Problem Solved: "+completed_successfully);
    }

    public void start(){
        long start_time, end_time, time_elapsed;
        System.out.println("Starting Problem "+problem_number);
        start_time = System.currentTimeMillis();

        int max = 1000000;
        int target = 10001;
        ArrayList<Integer> primes = generatePrimes(max);

        Collections.sort(primes);

        if(primes.size() >= target){
            System.out.println(primes.get(target-1));
        }
        else {
            System.out.println("Not enough primes");
        }

        end_time = System.currentTimeMillis();
        System.out.println("Finished Problem");
        time_elapsed = end_time - start_time;
        System.out.println("Time taken to finish problem: "+time_elapsed);
    }

    private ArrayList<Integer> generatePrimes(int upper_bound){
        boolean divisible;
        double bound;
        ArrayList<Integer> primeList = new ArrayList<>();

        //add two since it's the only even prime
        primeList.add(2);

        for(int i=3; i<upper_bound; i+=2){
            //find the factor bound
            bound = Math.sqrt(i);
            divisible = false;

            for(int j : primeList){
                //if 'j' is divisible, it's not a prime
                if(i%j == 0) {
                    divisible = true;
                    break;
                }
                //if the number we're checking is greater then the squareroot of 'i'
                //and we haven't found something divisible
                //then it's a prime by mathematical law
                else if((double)j >  bound) {
                    break;
                }
            }
            //if the number did not became divisible, record it
            if(!divisible) {
                primeList.add(i);
            }
        }

        return primeList;
    }
}