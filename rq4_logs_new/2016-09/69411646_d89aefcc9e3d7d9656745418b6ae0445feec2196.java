package Logics;

public class PrimeFind {
    
    
    public int getPrime(int size) // returns 1st prime > min
    {
        for (int j = size + 1; true; j++) // for all j > min
        {
            if (isPrime(j)) // is j prime?
            {
                return j; // yes, return it
            }
        }
    }
   
    private boolean isPrime(int n) // is n prime?
    {
        for (int j = 2; (j * j <= n); j++) // for all j
        {
            if (n % j == 0) // divides evenly by j?
            {
                return false; // yes, so not prime
            }
        }
        return true; // no, so prime
    }
}