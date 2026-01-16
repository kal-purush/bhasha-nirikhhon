using System;
using System.Collections.Concurrent;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace RXDemo.AsyncBehavior
{
    class Program
    {
        private static readonly Subject<Item> itemSubject = new();
        private static readonly BlockingCollection<Item> queue = new BlockingCollection<Item>(new ConcurrentQueue<Item>());
        private static readonly Random rnd = new Random();
        static void Main(string[] args)
        {
            Task.Factory.StartNew(()=>
            itemSubject.Subscribe(OnItemData));

            //Task.Factory.StartNew(InitProcessingJob);

            //for (int i = 0; i < 4; i++)
            //{
            //    Task.Run(AddItemsForProcessing);
            //}
            
            AddItemsForProcessing();
            Console.WriteLine("Main thread execution");
            Console.ReadKey();
            
        }

        private static void InitProcessingJob()
        {
            foreach (var item in queue.GetConsumingEnumerable())
            {
                ProcessItemData(item);
            }
        }

        private static void AddItemsForProcessing()
        {

            var item = new Item
            {
                Id = rnd.Next(0, 100),
                Name = "Test"
            };
            
            Console.WriteLine($"Processing item {item.Id}");
            itemSubject.OnNext(item);

            //queue.Add(item);
        }

        private static void OnItemData(Item obj)
        {
            ProcessItemData(obj);
        }

        private static void ProcessItemData(Item obj)
        {
            Thread.Sleep(10000);
            Console.WriteLine($"Processing Complete for item {obj.Id}");
        }
    }

    class Item
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}