using System.Threading;
using System.Threading.Tasks;

namespace AsyncAwait.Task1.CancellationTokens;

internal static class Calculator
{
    public static long Calculate(int n, CancellationToken cancellationToken)
    {
        long sum = 0;

        for (var i = 0; i < n; i++)
        {
            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();
            sum = sum + (i + 1);
            Thread.Sleep(10);
        }

        return sum;
    }
}