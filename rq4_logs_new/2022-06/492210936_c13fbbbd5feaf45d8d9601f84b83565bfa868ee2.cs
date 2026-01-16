using Simsprojekat.Controller;
using Simsprojekat.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Simsprojekat.Utils
{
    internal class ThreadCreator
    {
        private static ThreadCreator _instance = new ThreadCreator();

        public static void TollBoothThreadTask(TollStation ts)
        {
            TollBoothController tollBoothController = tollBoothController = new TollBoothController();
            List<TollBooth> tollBooths = tollBoothController.GetByTollStationId(ts.Id);
            while (true)
            {
                foreach (TollBooth tb in tollBooths)
                {
                    foreach (Device d in tb.Devices)
                    {
                        Random rnd = new Random();
                        if (rnd.Next(0, 50000) == 1)
                        {
                            d.Faulty = true;
                            tollBoothController.Update(tb);
                            Console.WriteLine("Device is faulty :" + d.Name + "  " + tb.Id);
                        }
                    }
                }
                Thread.Sleep(10);
            }
        }
    }

}