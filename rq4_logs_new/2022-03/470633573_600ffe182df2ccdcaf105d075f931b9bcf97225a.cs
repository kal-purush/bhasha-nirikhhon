using BenchmarkDotNet.Attributes;

namespace Test
{
    public class BenchmarkStringIntialization
    {

        [Benchmark]
        public void IntializeString()
        {
            string ss = "Test";
        }
        [Benchmark]
        public void Intializevar()
        {
            var ss = "Test";
        }

        [Benchmark]
        public void IntializeString1()
        {
            string ss = "Test";
        }
        [Benchmark]
        public void Intializevar1()
        {
            var ss = "Test";
        }



    }
}