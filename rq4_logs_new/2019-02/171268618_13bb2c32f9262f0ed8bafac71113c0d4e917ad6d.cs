using System;
using System.Net;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using NLog;
using NLog.Config;
using NLog.Targets;
using TaskZero.Repository.EventStore;

namespace TaskZero.Adapter.Host
{
    class Program
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        private static Uri _uri;

        static void Main(string[] args)
        {
            ConfigureLogging();

            var es = "localhost:1113";
            if (args.Length > 0)
                es = args[0];
            _uri = new Uri($"tcp://{es}");

            var domainConnection = BuildConnection("es-taskzero-domain", _uri);
            domainConnection.ConnectAsync().Wait();
            var inputConnection = BuildConnection("es-taskzero-input", _uri);
            inputConnection.ConnectAsync().Wait();
            var domainRepository = new EventStoreDomainRepository("domain", domainConnection);

            var endpoint = new EndPoint(inputConnection, domainRepository, new Handler(domainRepository),
                new UserCredentials("admin", "changeit"));
            endpoint.Start();

            Log.Info("Press enter to exit");
            Console.ReadLine();
        }

        private static IEventStoreConnection BuildConnection(string name, Uri uri)
        {
            return EventStoreConnection.Create(ConnectionSettings.Create().KeepRetrying().KeepReconnecting(), uri,
                name);
        }

        private static void ConfigureLogging()
        {
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget("target1")
            {
                //Layout = @"${date:format=HH\:mm\:ss} ${level} ${message} ${exception}"
            };
            config.AddTarget(consoleTarget);
            config.AddRuleForAllLevels(consoleTarget);
            LogManager.Configuration = config;
        }
    }
}