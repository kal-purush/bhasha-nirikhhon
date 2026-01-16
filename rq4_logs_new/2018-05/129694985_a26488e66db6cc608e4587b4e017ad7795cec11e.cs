using MultiThread;
using MultiThread.Throttle;
using System;
using System.Timers;
using System.Threading.Tasks;
using System.Threading;
using Timer = System.Timers.Timer;

namespace Arsenal
{
    class ThrottlingDemo
    {
        ThrottleController controller;

        private static volatile int value1, value2;
        
        public void Run()
        {
            ThrottleConfig config = new ThrottleConfig(new int[] { 10, 20})
            {
                RejectionFactor = 3,
                TokenExpiration = TimeSpan.FromSeconds(5)
            };

            controller = new ThrottleController(config);
            controller.Start();

            value1 = 0;
            value2 = 0;
            Timer timer = new Timer(50);
            timer.Enabled = true;
            timer.Elapsed += (source, stat) => QueueWorkItems(controller);
            Thread.Sleep(TimeSpan.FromMinutes(10));
        }

        private class Worker : IThrottableProcess
        {
            private readonly int id;

            public Worker(int id)
            {
                this.id = id;
            }

            public Task Execute()
            {
                return Task.Run(() =>
                {
                    if (id == 0)
                        Interlocked.Increment(ref ThrottlingDemo.value1);
                    else
                        Interlocked.Increment(ref ThrottlingDemo.value2);
                });
            }

            public int GetQuotaId() => id;
        }

        private void QueueWorkItems(IThrottleController controller)
        {
            controller.QueueProcess(new Worker(0));
            controller.QueueProcess(new Worker(1));
            Console.WriteLine($"Current: {value1}, {value2}");
        }
    }
}