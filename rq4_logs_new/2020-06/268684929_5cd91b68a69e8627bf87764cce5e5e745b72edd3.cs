namespace Tom.Lib.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            Clusters.IReadWritingHandler handler = new Clusters.ReadWritingHandlerDemo();
            var cluster = new Clusters.ReadWritingCluster(handler);
            cluster.Start();
        }
    }
}