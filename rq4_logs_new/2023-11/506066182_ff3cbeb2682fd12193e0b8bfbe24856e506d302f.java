import java.util.Arrays;

public class PrimeNumberCounter {
    public int solution(int number, int k) {
        String kNaryNumber = toKNaryNumber(number, k);

        String[] split = split(kNaryNumber);

        return countPrimeNumbers(split);
    }

    public String toKNaryNumber(int number, int k) {
        return Integer.toString(number, k);
    }

    public String[] split(String kNary) {
        return kNary.split("0");
    }

    public int countPrimeNumbers(String[] split) {
        return (int) Arrays.stream(split)
            .filter(number -> !number.equals(""))
            .map(Long::parseLong)
            .filter(this::isPrimeNumber)
            .count();
    }

    public boolean isPrimeNumber(long number) {
        if (number == 1) {
            return false;
        }

        if (number == 2) {
            return true;
        }

        long range = (long) Math.ceil(Math.sqrt(number));
        for (int divisor = 3; divisor <= range; divisor += 1) {
            if (number % divisor == 0) {
                return false;
            }
        }

        return true;
    }
}