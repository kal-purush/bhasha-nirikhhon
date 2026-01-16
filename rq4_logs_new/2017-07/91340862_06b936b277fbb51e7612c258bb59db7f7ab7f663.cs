using NetworkAndGenericCalculation.Worker;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NetworkAndGenericCalculation.Node
{
    //Node running on the computer where is it launched.
    public class LocalNode : INode
    {


        //Process p = /*get the desired process here*/;
        //private PerformanceCounter ramCounter = new PerformanceCounter("Process", "Working Set", p.ProcessName);
        //private PerformanceCounter cpuCounter = new PerformanceCounter("Process", "% Processor Time", p.ProcessName);

        private PerformanceCounter processorCounter;
        private PerformanceCounter memoryCounter;

        public LocalNode(int localThreadsCount)
                {
                    Workers = new WThread[localThreadsCount];
                    for (int i = 0; i < localThreadsCount; ++i)
                    {
                        Workers[i] = new WThread(this, i);
                    }
                    processorCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
                    memoryCounter = new PerformanceCounter("Memory", "Available MBytes");
                }
        //while (true)
        //{
        //    Thread.Sleep(500);
        //    double ram = ramCounter.NextValue();
        //double cpu = cpuCounter.NextValue();
        //Console.WriteLine("RAM: "+(ram/1024/1024)+" MB; CPU: "+(cpu)+" %");
        //}

  

        public IList<IWorker> Workers { get; protected set; }


        public string IpAdress => "IPBouchon";
    
        // Filter a sequence of valor following a predicate
        public int ActualWorker => Workers.Where(workers => ! workers.IsAvailable).Count();

        public float ProcessorUsage => processorCounter.NextValue();

        public float MemoryUsage => memoryCounter.NextValue();

        public override string ToString() => "LocalNode [" + IpAdress + "]";
    }
}