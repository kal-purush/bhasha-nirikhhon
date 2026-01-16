using BenchmarkDotNet.Attributes;
using System.Threading.Tasks;
using aoc.day2;
using aoc.day3;
using aoc.day4;
using aoc.day5;

namespace aoc.benchmarks
{
    public class DayBenchmarks
    {
        private const string BasePath = "day";
        private const string Extension = ".txt";

        [Benchmark]
        public async Task<string> Day1Benchmark()
        {
            return await Day1.RunAsync(BasePath + 1 + Extension);
        }

        [Benchmark]
        public async Task<int> Day2Part1Benchmark()
        {
            return await Day2.Part1CalculationWithStreamReader(BasePath + 2 + Extension);
        }

        [Benchmark]
        public async Task<int> Day2Part2Benchmark()
        {
            return await Day2.Part2Calculation(BasePath + 2 + Extension);
        }

        [Benchmark]
        public int Day3Part1Benchmark()
        {
            return Day3.Part1CalculationFile(BasePath + 3 + Extension);
        }

        [Benchmark]
        public int Day3Part2Benchmark()
        {
            return Day3.Part2CalculationFile(BasePath + 3 + Extension);
        }

        [Benchmark]
        public int Day4Part1Benchmark()
        {
            var day4 = new Day4(BasePath + 4 + Extension);
            return day4.Part1Count;
        }

        [Benchmark]
        public int Day4Part2Benchmark()
        {
            var day4 = new Day4(BasePath + 4 + Extension);
            return day4.Part2Count;
        }

        [Benchmark]
        public int Day5Part1Benchmark()
        {
            var day5 = new Day5(BasePath + 5 + Extension);
            return day5.Part1Result;
        }

        [Benchmark]
        public int Day5Part2Benchmark()
        {
            var day5 = new Day5(BasePath + 5 + Extension);
            return day5.Part2Result;
        }
    }
}