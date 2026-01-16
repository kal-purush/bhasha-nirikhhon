using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Arsenal.CombinerDemo
{
    /*
     Arsenal.exe --MaxEventsPerBatch 100 --NumBuckets 100 
                 --WordCountFolder D:\Coding\Git\HeavenRepo\csharp\Arsenal\Arsenal\bin\Debug\Words 
                 --AggregateLevel 2<change this value to 1,2,3> --TotalDifferentWords 20<adjust this for number of words>
                 --OccuranceOfSkewWords 100000<adjust this for number of words> --Interval 00:00:01
         */
    class WordCountCombinerDemo
    {
        public int MaxEventsPerBatch { get; set; }
        public int NumBuckets { get; set; }
        public string WordCountFolder { get; set; }
        public int AggregateLevel { get; set; }
        public int TotalDifferentWords { get; set; }
        public int OccuranceOfSkewWords { get; set; }
        public TimeSpan Interval { get; set; }

        public volatile bool Running = false;
        List<Task> reducerTasks;
        private WordCountReducer reducer;
        private WordCountEventQueue queue;
        private List<int> bucketProcessingCount;

        public void Run(string[] args)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i += 2)
            {
                dict[args[i]] = args[i + 1];
            }

            var properties = GetType().GetProperties();
            foreach (var property in properties)
            {
                string name = property.Name;
                string value = dict["--" + name];
                object parsedValue = null;
                if (property.PropertyType == typeof(string))
                {
                    parsedValue = value;
                }
                else if (property.PropertyType == typeof(int))
                {
                    parsedValue = int.Parse(value);
                }
                else if (property.PropertyType == typeof(TimeSpan))
                {
                    parsedValue = TimeSpan.Parse(value);
                }

                property.SetValue(this, parsedValue);
            }

            Start().Wait();
        }

        public async Task Start()
        {
            reducer = new WordCountReducer(AggregateLevel, WordCountFolder);
            queue = new WordCountEventQueue(NumBuckets);
            bucketProcessingCount = Enumerable.Repeat(0, NumBuckets).ToList();
            reducer.Queue = queue;
            queue.Reducer = reducer;
            Running = true;

            reducerTasks = Enumerable.Range(0, NumBuckets).Select(ReduceUntilFinished).ToList();
            Task generateEventsTask = GenerateEventsTask();
            
            await generateEventsTask;
            Console.WriteLine("generating process finished!");

            Task resetRunning = ResetRunningWhenQueuesAreEmpty();
            await Task.WhenAll(reducerTasks);

            await resetRunning;
            Console.WriteLine("all reducing tasks finished!");
            Console.WriteLine(string.Join(", ", bucketProcessingCount));
        }

        public async Task GenerateEventsTask()
        {
            Console.WriteLine("Word generation started!");
            Random rand = new Random();
            for (int i = 0; i < OccuranceOfSkewWords; i++)
            {
                for (int j = 0; j < TotalDifferentWords; j++)
                {
                    if (j >= 5)
                    {
                        if (rand.NextDouble() > 0.1)
                            continue;
                    }

                    string word = new string((char)((int)'A' + j), 10);
                    queue.EnqueueEvent(new WordCountEvent(word));
                }

                if (i % 1000 == 1)
                    await Task.Delay(1);
            }
        }

        public async Task ResetRunningWhenQueuesAreEmpty()
        {
            while (!this.queue.Buckets.All(q => q.IsEmpty))
            {
                await Task.Delay(1);
            }

            Running = false;
        }

        public async Task ReduceUntilFinished(int bucketId)
        {
            DateTime reduceStartTime = DateTime.Now;
            Console.WriteLine($"Reduce on bucket {bucketId} started");
            while (Running)
            {
                DateTime timeStart = DateTime.UtcNow;
                AggregateOnce(bucketId);
                Console.WriteLine($"{bucketId} cost: {DateTime.Now - reduceStartTime}");

                DateTime timeFinished = DateTime.UtcNow;
                TimeSpan intervalLeft = Interval - (timeFinished - timeStart);
                if (intervalLeft > TimeSpan.Zero)
                {
                    await Task.Delay(intervalLeft);
                }
            }
        }
                
        public void AggregateOnce(int bucketId)
        {
            var wordQueue = queue.Buckets[bucketId];
            List<WordCountEvent> events = new List<WordCountEvent>();
            WordCountEvent evt;
            while (events.Count < MaxEventsPerBatch && wordQueue.TryDequeue(out evt))
            {
                events.Add(evt);
            }
            bucketProcessingCount[bucketId] += events.Count;

            var groups = events.GroupBy(wordCountEvt => Tuple.Create(wordCountEvt.Word, wordCountEvt.GetAggregationLevel()));
            foreach (var group in groups)
            {
                reducer.Process(group);
            }
        }
    }
}