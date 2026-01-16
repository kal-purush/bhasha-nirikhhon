using BenchmarkDotNet.Attributes;
using EntityDatabaseFirst.Function;

namespace EntityDatabaseFirst
{
    public class BenchMarkService
    {
        [Params(5, 10)]
        public int IterationCount;

        [Benchmark]
        public void RunAdoNet()
        {
            var adoConnectService = new AdoService(); //Ado.net
            for (int i = 0; i < IterationCount; i++)
                adoConnectService.Run();
        }
        [Benchmark]
        public void RunEFCore()
        {
            var postService = new PostService(); //Entity framrework core
            for (int i = 0; i < IterationCount; i++)
                postService.Run();
        }
    }
}