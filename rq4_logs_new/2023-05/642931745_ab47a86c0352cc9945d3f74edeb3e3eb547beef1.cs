using BenchmarkDotNet.Attributes;
using System.Text;

namespace DemoCryptoLib.Benchmarks;

public class PasswordHashGeneratorBenchmark
{
    [Params(100, 1000, 10000)]
    public int IterationCount;

    [Benchmark(Baseline = true)]
    public string Base()
    {
        const string password = "dummyPassword";
        byte[] salt = Encoding.UTF8.GetBytes("dummySalt01234567890");

        return DemoCryptoLib.GeneratePasswordHashUsingSalt(password, salt);
    }

    [Benchmark]
    public string Optimized()
    {
        const string password = "dummyPassword";
        byte[] salt = Encoding.UTF8.GetBytes("dummySalt01234567890");

        return DemoCryptoLib.GeneratePasswordHashUsingSaltOptimized(password, salt);
    }
}