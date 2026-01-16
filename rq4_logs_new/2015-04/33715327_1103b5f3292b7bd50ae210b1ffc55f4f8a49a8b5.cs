using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.ServiceModel;
using WcfFynbusService;

namespace HostServies
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("[" + DateTime.Now.ToString() + "] Setter services up... ");
            var login = new ServiceHost(typeof(WcfFynbusService.Services.Login));
            var offersform = new ServiceHost(typeof(WcfFynbusService.Services.Offersform_SingleVehicle));
            var inhous = new ServiceHost(typeof(WcfFynbusService.Services.Inhous));
            var admin = new ServiceHost(typeof(WcfFynbusService.Services.Admin));
            Console.WriteLine("[Done]");

            Console.Write("[" + DateTime.Now.ToString() + "] Starter services... ");
            login.Open();
            offersform.Open();
            inhous.Open();
            admin.Open();
            Console.WriteLine("[Done]");

            Console.WriteLine("Tryk på enter for at lukke");
            Console.ReadLine();
            Console.Write("Lukker");
            login.Close();
            Console.Write(".");
            offersform.Close();
            Console.Write(".");
            inhous.Close();
            Console.Write(".");
            admin.Close();
        }
    }
}