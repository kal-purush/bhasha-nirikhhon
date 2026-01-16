using Controller.Services;
using Controller.Services.Admin;
using MEFLoader;
using Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Test;

namespace Controller
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting server initialization...");

            Console.WriteLine("\tStarting MEF catalog...");
            Loader loader = new Loader();
            loader.LoadCatalog();

            Console.WriteLine("\tRegistering connectors...");
            foreach (var i in loader.Test) { TestObject.Add(i); }
            foreach (var i in loader.Weather) { WeatherObject.Add(i); }
            foreach (var i in loader.Kodi) { KodiObject.Add(i); }
            foreach (var i in loader.GenericInfo) { GenericInfoObject.Add(i); }

            Console.WriteLine("\tLoading business rules...");
            BusinessRules.BusinessRules.Initialize();

            Console.WriteLine("\tCollecting admin methods...");
            BusinessRules.DataCollector.Collector.Collect();

            Console.WriteLine("\tHosting web services...");
            new ServiceHost(typeof(TestService)).Open();
            new ServiceHost(typeof(KodiService)).Open();
            new ServiceHost(typeof(WeatherService)).Open();
            new ServiceHost(typeof(GenericInfoService)).Open();
            new ServiceHost(typeof(AdminService)).Open();

            Console.WriteLine("Server initialization complete");
            Console.WriteLine("------------------------------\n");
            Console.ReadLine();
        }
    }
}