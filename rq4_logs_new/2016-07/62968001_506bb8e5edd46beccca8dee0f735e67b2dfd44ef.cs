using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

namespace ProducerConsumer
{
    class Program
    {
        static object _storageSynchronizationObject = new object();
        static object _producerSynchronizationObject = new object();
        static object _consumerSynchronizationObject = new object();

        static Random random = new Random();

        private static Thread _producerThread = null;
        private static Thread _consumerThread = null;

        const int MaxProductionTime = 1000;
        const int MaxConsumptionTime = 1000;
        const int MaxProductionStartTime = 5000;
        const int MaxConsumptionStartTime = 5000;

        private static int _storageCapacity = 20;
        private static int _storageElementCounter = 1;

        static void DisplayStorageInfo()
        {
            Console.WriteLine("Storage Info\n\tStorage Capacity : {0}\n\tElements in storage : {1}\n\tFilled : {2} %", _storageCapacity, _storageElementCounter, (double)_storageElementCounter/_storageCapacity *100);
        }
        static void Main(string[] args)
        {
            ThreadStart producerAction = () =>
            {
                Console.WriteLine("Starting producer thread");
                while (true)
                {
                    lock (_storageSynchronizationObject)
                    {
                        _storageElementCounter++;
                        Console.WriteLine("Added element into storage");
                    }
                    DisplayStorageInfo();
                    if (_storageElementCounter >= _storageCapacity)
                    {
                        Console.WriteLine("Producer thread paused");
                        lock (_producerSynchronizationObject) Monitor.Wait(_producerSynchronizationObject);
                        Console.WriteLine("Producer thread to be resumed");
                        Thread.Sleep(random.Next(MaxProductionStartTime));
                        Console.WriteLine("Producer thread resumed");
                        Thread.Sleep(MaxProductionTime);
                    }
                    lock(_consumerSynchronizationObject) Monitor.Pulse(_consumerSynchronizationObject);
                    Thread.Sleep(random.Next(MaxProductionTime));
                }
            };

            ThreadStart consumerAction = () =>
            {
                Console.WriteLine("Starting consumer thread");
                while (true)
                {
                    lock (_storageSynchronizationObject)
                    {
                        _storageElementCounter--;
                        Console.WriteLine("Element taken from storage");
                    }
                    DisplayStorageInfo();
                    if (_storageElementCounter <= 0)
                    {
                        Console.WriteLine("Consumer thread paused");
                        lock (_consumerSynchronizationObject) Monitor.Wait(_consumerSynchronizationObject);
                        Console.WriteLine("Consumer thread to be resumed");
                        Thread.Sleep(random.Next(MaxConsumptionStartTime));
                        Console.WriteLine("Consumer thread resumed");
                    }
                    lock(_producerSynchronizationObject) Monitor.Pulse(_producerSynchronizationObject);
                    Thread.Sleep(random.Next(MaxConsumptionTime));
                }
            };
            _producerThread = new Thread(producerAction);
            _producerThread.IsBackground = true;
            _producerThread.Start();

            _consumerThread = new Thread(consumerAction);
            _consumerThread.IsBackground = true;
            _consumerThread.Start();

            Console.ReadLine();
            _producerThread.Abort();
            _consumerThread.Abort();
            Console.WriteLine("End");
            DisplayStorageInfo();
            Console.ReadLine();
        }
    }
}