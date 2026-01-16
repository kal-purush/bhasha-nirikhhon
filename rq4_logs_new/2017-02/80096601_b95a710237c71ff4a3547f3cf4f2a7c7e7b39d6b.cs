using CommandLine;
using System;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication
{
    public class Program
    {
        private static HttpClient[] _httpClients;
        private static long _httpClientCounter = 0;
        private static int[] _queuedRequests;

        private const string _payload =
            @"{ ""data"": ""{'job_id':'c4bb6d130003','container_id':'ab7b85dcac72','status':'Success: process exited with code 0.'}"" }";

        private static Stopwatch _stopwatch = Stopwatch.StartNew();

        private static long _requests;
        private static long _ticks;

        private static Options _options;

        private class Options
        {
            [Option('u', "uri", Required = true)]
            public Uri Uri { get; set; }

            [Option('m', "method", Default = HttpMethod.Get)]
            public HttpMethod Method { get; set; }

            [Option('p', "parallel", Default = 512)]
            public int Parallel { get; set; }

            [Option('t', "threadingMode", Default = ThreadingMode.Task)]
            public ThreadingMode ThreadingMode { get; set; }

            [Option('c', "clients", Default = 1)]
            public int Clients { get; set; }

            [Option('e', "expectContinue")]
            public bool? ExpectContinue { get; set; }

            [Option('r', "requests", Default = long.MaxValue)]
            public long Requests { get; set; }

            [Option('s', "clientSelectionMode", Default = ClientSelectionMode.TaskRoundRobin)]
            public ClientSelectionMode ClientSelectionMode { get; set; }

            [Option("minQueue", Default = 0)]
            public int MinQueue { get; set; }

            [Option("maxQueue", Default = int.MaxValue)]
            public int MaxQueue { get; set; }

            [Option('v', "verbose", Default = false)]
            public bool Verbose { get; set; }
        }

        private enum HttpMethod
        {
            Get,
            Post
        }

        private enum ThreadingMode
        {
            Task,
            Thread
        }

        private enum ClientSelectionMode
        {
            TaskRoundRobin,
            TaskRandom,
            RequestRoundRobin,
            RequestRandom,
            RequestShortestQueue,
            RequestRandomNotLongestQueue,
            RequestRandomTolerance
        }

        public static int Main(string[] args)
        {
            var parser = new Parser(settings =>
            {
                settings.CaseInsensitiveEnumValues = true;
                settings.HelpWriter = Console.Out;
            });

            return parser.ParseArguments<Options>(args).MapResult(
                options => Run(options),
                _ => 1
            );
        }

        private static int Run(Options options)
        {
            _options = options;

            Console.WriteLine(
                $"{options.Method.ToString().ToUpperInvariant()} {options.Uri} with " +
                $"{options.Parallel} {options.ThreadingMode.ToString().ToLowerInvariant()}(s), " +
                $"{options.Clients} client(s), " +
                $"ClientSelectionMode={options.ClientSelectionMode.ToString()}, " +
                $"MinQueue={options.MinQueue}, MaxQueue={options.MaxQueue}, " +
                $"and ExpectContinue={options.ExpectContinue?.ToString() ?? "null"}" +
                "...");
            
            Console.WriteLine($"Server GC = {GCSettings.IsServerGC}");

            var writeResultsTask = WriteResults();

            RunTest();

            writeResultsTask.Wait();

            return 0;
        }

        private static void RunTest()
        {
            _queuedRequests = new int[_options.Clients];

            _httpClients = new HttpClient[_options.Clients];
            for (int i = 0; i < _options.Clients; i++)
            {
                _httpClients[i] = new HttpClient(new HttpClientHandler() {
                    ServerCertificateCustomValidationCallback = (a, b, c, d) => true
                });
            }

            if (_options.ThreadingMode == ThreadingMode.Thread)
            {
                var threads = new Thread[_options.Parallel];

                for (var i = 0; i < _options.Parallel; i++)
                {
                    int clientId = -1;
                    if (_options.ClientSelectionMode == ClientSelectionMode.TaskRoundRobin)
                    {
                        clientId = i % _httpClients.Length;
                    }
                    else if (_options.ClientSelectionMode == ClientSelectionMode.TaskRandom)
                    {
                        clientId = ConcurrentRandom.Next() % _httpClients.Length;
                    }

                    var thread = new Thread(() =>
                    {
                        long requestId;
                        while ((requestId = Interlocked.Increment(ref _requests)) <= _options.Requests)
                        {
                            var start = _stopwatch.ElapsedTicks;
                            using (var response = ExecuteRequestAsync(requestId, clientId).Result) { }
                            var end = _stopwatch.ElapsedTicks;

                            Interlocked.Add(ref _ticks, end - start);
                        }
                        Interlocked.Decrement(ref _requests);
                    });
                    threads[i] = thread;
                    thread.Start();
                }

                for (var i = 0; i < _options.Parallel; i++)
                {
                    threads[i].Join();
                }
            }
            else if (_options.ThreadingMode == ThreadingMode.Task)
            {
                var tasks = new Task[_options.Parallel];
                for (var i = 0; i < _options.Parallel; i++)
                {
                    int clientId = -1;
                    if (_options.ClientSelectionMode == ClientSelectionMode.TaskRoundRobin)
                    {
                        clientId = i % _httpClients.Length;
                    }
                    else if (_options.ClientSelectionMode == ClientSelectionMode.TaskRandom)
                    {
                        clientId = ConcurrentRandom.Next() % _httpClients.Length;
                    }
                    var task = ExecuteRequestsAsync(clientId);
                    tasks[i] = task;
                }

                Task.WaitAll(tasks);
            }
            else
            {
                throw new InvalidOperationException();
            }
        }

        private static int NextClientId()
        {
            return (int)(Interlocked.Increment(ref _httpClientCounter) % _httpClients.Length);
        }

        private static int ShortestQueue()
        {
            var shortestQueue = 0;
            var shortestQueueLength = _queuedRequests[0];

            for (var i = 1; i < _httpClients.Length; i++)
            {
                var queueLength = _queuedRequests[i];
                if (queueLength < shortestQueueLength)
                {
                    shortestQueue = i;
                    shortestQueueLength = queueLength;
                }
            }

            return shortestQueue;
        }

        private static int LongestQueue()
        {
            var longestQueue = 0;
            var longestQueueLength = _queuedRequests[0];

            for (var i = 1; i < _httpClients.Length; i++)
            {
                var queueLength = _queuedRequests[i];
                if (queueLength > longestQueueLength)
                {
                    longestQueue = i;
                    longestQueueLength = queueLength;
                }
            }

            return longestQueue;
        }

        private static async Task<HttpResponseMessage> ExecuteRequestAsync(long requestId, int clientId)
        {
            if (_options.ClientSelectionMode == ClientSelectionMode.RequestRoundRobin)
            {
                clientId = NextClientId();
            }
            else if (_options.ClientSelectionMode == ClientSelectionMode.RequestRandom)
            {
                clientId = ConcurrentRandom.Next() % _httpClients.Length;
            }
            else if (_options.ClientSelectionMode == ClientSelectionMode.RequestShortestQueue)
            {
                clientId = ShortestQueue();
            }
            else if (_options.ClientSelectionMode == ClientSelectionMode.RequestRandomNotLongestQueue)
            {
                var longestQueue = LongestQueue();

                int random;
                do
                {
                    random = ConcurrentRandom.Next() % _httpClients.Length;
                }
                while (random == longestQueue);

                clientId = random;
            }
            else if (_options.ClientSelectionMode == ClientSelectionMode.RequestRandomTolerance)
            {
                // If the shortest queue is below the minimum, select it.  Else, select a random queue below the maximum.

                var shortestQueue = ShortestQueue();                
                if (_queuedRequests[shortestQueue] < _options.MinQueue)
                {
                    clientId = shortestQueue;
                }
                else
                {
                    int random;
                    do
                    {
                        random = ConcurrentRandom.Next() % _httpClients.Length;
                    }
                    while (_queuedRequests[random] >= _options.MaxQueue);

                    clientId = random;
                }
            }

            var managedThreadIdBefore = _options.Verbose ? Thread.CurrentThread.ManagedThreadId : -1;

            var httpClient = _httpClients[clientId];
            HttpResponseMessage response;

            Interlocked.Increment(ref _queuedRequests[clientId]);

            if (_options.Method == HttpMethod.Get)
            {
                response = await httpClient.GetAsync(_options.Uri);
            }
            else if (_options.Method == HttpMethod.Post)
            {
                using (var m = new HttpRequestMessage(System.Net.Http.HttpMethod.Post, _options.Uri))
                using (var content = new StringContent(_payload))
                {
                    m.Content = content;
                    m.Headers.ExpectContinue = _options.ExpectContinue;
                    response = await httpClient.SendAsync(m);
                }
            }
            else
            {
                throw new InvalidOperationException();
            }

            Interlocked.Decrement(ref _queuedRequests[clientId]);

            if (_options.Verbose)
            {
                Console.Error.WriteLine($"RequestId:{requestId}, ClientId:{clientId}, " +
                    $"ThreadIdBefore: {managedThreadIdBefore} ThreadIdAfter:{Thread.CurrentThread.ManagedThreadId}");
            }

            return response;
        }

        private static async Task ExecuteRequestsAsync(int clientId)
        {
            long requestId;
            while ((requestId = Interlocked.Increment(ref _requests)) <= _options.Requests)
            {
                var start = _stopwatch.ElapsedTicks;
                using (var response = await ExecuteRequestAsync(requestId, clientId))
                {
                }
                var end = _stopwatch.ElapsedTicks;

                Interlocked.Add(ref _ticks, end - start);
            }
            Interlocked.Decrement(ref _requests);
        }

        private static async Task WriteResults()
        {
            var lastRequests = (long)0;
            var lastTicks = (long)0;
            var lastElapsed = TimeSpan.Zero;

            do
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                var requests = _requests;
                var currentRequests = requests - lastRequests;
                lastRequests = requests;

                var ticks = _ticks;
                var currentTicks = ticks - lastTicks;
                lastTicks = ticks;

                var elapsed = _stopwatch.Elapsed;
                var currentElapsed = elapsed - lastElapsed;
                lastElapsed = elapsed;

                WriteResult(requests, ticks, elapsed, currentRequests, currentTicks, currentElapsed);
            }
            while (Interlocked.Read(ref _requests) < _options.Requests);
        }

        private static void WriteResult(long totalRequests, long totalTicks, TimeSpan totalElapsed,
            long currentRequests, long currentTicks, TimeSpan currentElapsed)
        {
            var totalMs = ((double)totalTicks / Stopwatch.Frequency) * 1000;
            var currentMs = ((double)currentTicks / Stopwatch.Frequency) * 1000;

            Console.WriteLine(
                $"{DateTime.UtcNow.ToString("o")}\tTot Req\t{totalRequests}" +
                $"\tCur RPS\t{Math.Round(currentRequests / currentElapsed.TotalSeconds)}" +
                $"\tCur Lat\t{Math.Round(currentMs / currentRequests, 2)}ms" +
                $"\tAvg RPS\t{Math.Round(totalRequests / totalElapsed.TotalSeconds)}" +
                $"\tAvg Lat\t{Math.Round(totalMs / totalRequests, 2)}ms" +
                $"\tReq\t{String.Join(" ", _queuedRequests)}"
            );
        }

        public static class ConcurrentRandom
        {
            private static Random _global = new Random();

            [ThreadStatic]
            private static Random _local;

            public static int Next()
            {
                Random inst = _local;
                if (inst == null)
                {
                    int seed;
                    lock (_global) seed = _global.Next();
                    _local = inst = new Random(seed);
                }
                return inst.Next();
            }
        }
    }
}