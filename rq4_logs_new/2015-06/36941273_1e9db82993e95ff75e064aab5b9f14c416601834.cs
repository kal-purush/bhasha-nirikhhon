using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Util;

namespace ConsumerProcess
{
    class Program
    {


        static void Main(string[] args)
        {
            int DemoTime = int.Parse(ConfigurationManager.AppSettings["DemoTime"]);
            int DelayMin = int.Parse(ConfigurationManager.AppSettings["DelayMin"]);
            int DelayMax = int.Parse(ConfigurationManager.AppSettings["DelayMax"]);
            string timestamp;
            Random rnd = new Random();
            Stopwatch sw = new Stopwatch();

            //Semaphores are opened for using
            Semaphore b = Semaphore.OpenExisting("Busy");
            Semaphore e = Semaphore.OpenExisting("Empty");
            Semaphore f = Semaphore.OpenExisting("Full");
            Semaphore l = Semaphore.OpenExisting("LogAccess");

            //Process will be killed when it is reached DemoTime
            sw.Start();
            while (sw.ElapsedMilliseconds < DemoTime)
            {
                bool exit = false;
                int nProcessID = Process.GetCurrentProcess().Id;

                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  is waiting for a transaction.");
                l.Release();

                Console.WriteLine("Waiting for new transactions.");
                while (!f.WaitOne(5))
                {
                    exit = sw.ElapsedMilliseconds > DemoTime;
                    if (exit)
                        break;
                }

                if (exit)
                {
                    break;
                }
                Console.WriteLine("Transactions available. Good!!!");

                Console.WriteLine("Waiting for storage access.");

                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  is waiting for a storage access.");
                l.Release();

                b.WaitOne();
                Console.WriteLine("Storage access granted.");

                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  has granted access.");
                l.Release();

                // put
                var client = new NamedPipeClientStream("DataServer");
                client.Connect();

                StreamReader reader = new StreamReader(client);
                StreamWriter writer = new StreamWriter(client);

                //Call for transcation from PipeClientServer
                writer.WriteLine("GET");
                writer.Flush();

                string response = reader.ReadLine();
                byte[] data = Convert.FromBase64String(response);
                
                client.Close();
                client.Dispose();

                Console.WriteLine("Releasing storage access.");
                b.Release();
                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  released storage access.");
                l.Release();

                Console.WriteLine("Storage access released.");

                Console.WriteLine("Adding empty slot.");
                int empty = e.Release();
                Console.WriteLine("Now there are {0} empty slots.", empty);
                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  added an empty to buffer.");
                l.Release();

                // consume the transaction
                Console.WriteLine("Consuming the transaction.");

                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  started consuming the transaction.");
                l.Release();

                Thread.Sleep(rnd.Next(DelayMin, DelayMax));
                short dataLength = BitConverter.ToInt16(data, 0);
                
                Console.WriteLine(Encoding.UTF8.GetString(data, 20, dataLength));
                Console.WriteLine("Transaction consumed.");
                Console.WriteLine("---------------------------------------");

                l.WaitOne();
                timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss:fff", CultureInfo.InvariantCulture);
                Log.SaveLog(timestamp + " " + nProcessID + " ConsumerProcess  has transcation consumed.");
                l.Release();

            }
        }
    }
}