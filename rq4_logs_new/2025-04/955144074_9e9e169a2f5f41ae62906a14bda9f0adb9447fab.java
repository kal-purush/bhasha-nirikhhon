package begayim.recursion1;

public class rec_4_6 {

    public int bunnyEars2 (int bunnies){
        if (bunnies == 0){
            return 0;
        }
        if (bunnies % 2 == 1){
            return 2 + bunnyEars2(bunnies - 1);
        }
        else {
            return 3 + bunnyEars2(bunnies - 1);
        }
    }

    public int triangle (int rows){
        if (rows == 0){
            return 0;
        }
        return rows + triangle(rows - 1);
    }

    public int sumDigits (int n){
        if (n == 0){
            return 0;
        }
        return (n % 10) + sumDigits(n / 10);
    }
}