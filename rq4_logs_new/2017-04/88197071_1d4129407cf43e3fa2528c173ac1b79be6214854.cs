using ProjectEuler.Support;

public class Euler003
{
    private long maxValue;

    public Euler003(long maxValue)
    {
        this.maxValue = maxValue;
    }

    public long Solve()
    {
        GenPrimes primeTest = new GenPrimes();
        long currentVal = this.maxValue;

        for (long i = 2; i <= currentVal; i++)
            if (currentVal % i == 0)
            {
                if (primeTest.IsPrime(i))
                {
                    if (i == currentVal)
                    {
                        return (currentVal);
                    }
                    currentVal = currentVal / i;
                }
            }
        return (0);

    }

}