using ProjectEuler.Support;

namespace ProjectEuler
{
    class Euler007
    {
        private long nthPrime;

        public Euler007(long nthPrime)
        {
            this.nthPrime = nthPrime;
        }

        public long Solve()
        {
            GenPrimes prime_library = new GenPrimes();
            byte[] primes = prime_library.GetPrimes(1000000);
            int result = 0;
            for (int i = 0; i < 1000000; i++)
            {
                if (primes[i] == 1)
                {
                    result += 1;
                    if (result == nthPrime)
                    {
                        return (i);
                    }
                }

            }
            return (0);

        }
    }
}