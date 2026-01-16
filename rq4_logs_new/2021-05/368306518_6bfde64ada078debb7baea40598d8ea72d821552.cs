using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Async_Flow
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("<<<<<<<<<<<<<<<<<<<<<< moving next within Pending Tasks>>>>>>>>>>>>>>>>>");
            IAsyncEnumerable<int> IAsyncPendingTasks = pendingTask(1, 5);       
        }

        //pending tasks, running through a loop to complete tasks

        static async IAsyncEnumerable<int> pendingTask(int start, int iterator)
        {
            for(int i=0; i<iterator;i++)
            {
                await Task.Delay(2000);
                yield return start = +i;
            }
        }
    }
}