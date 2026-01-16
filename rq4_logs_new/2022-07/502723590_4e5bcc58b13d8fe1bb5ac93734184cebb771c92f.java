package File_work.Containers;


import java.util.stream.IntStream;

public class task_2 {

    private static int isPrime(int i) {
        return String.valueOf(i).chars().map(c -> c - 48).reduce(Integer::sum).getAsInt();
    }

    public static void printAllPrimes(int from, int to, int q) {
        IntStream primes = IntStream.range(from, to + 1).filter(k -> q == isPrime(k));
        primes.forEach(System.out::println);
    }



    public static void main(String[] args) {
        printAllPrimes(1, 45, 5);
    }
}