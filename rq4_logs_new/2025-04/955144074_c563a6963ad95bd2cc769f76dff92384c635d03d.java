package begayim.recursion1;

public class rec_7_9 {

    public int count7(int n){
        if (n == 0){
            return 0;
        }
        return (n % 10 == 7 ? 1 : 0) + count7(n / 10);
    }

    public int count8(int n){
        if (n == 0){
            return 0;
        }
        int lastDigit = n % 10;
        int nextDigit = (n / 10) % 10;

        if (lastDigit == 8) {
            if (nextDigit == 8) {
                return 2 + count8(n / 10);
            } else {
                return 1 + count8(n / 10);
            }
        }
        else {
                return count8(n / 10);
            }
    }

    public int powerN(int base, int n){
        if (n == 1){
            return base;
        }
        return base * powerN(base, n - 1);
    }
}
