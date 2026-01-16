using System;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Security.Permissions;

namespace MononokeEngine.Logging.ConsoleLogger
{
    public class ConsoleLoggerClient
    {
        [DllImport("kernel32")]
        static extern bool AllocConsole();
        
        
        [SecurityPermission(SecurityAction.Demand)]
        public void Run()
        {
            AllocConsole();
            
            
            // Create the channel.
            TcpChannel clientChannel = new TcpChannel();
            

            // Register the channel.
            ChannelServices.RegisterChannel(clientChannel, true);

            // Register as client for remote object.
            WellKnownClientTypeEntry remoteType = new WellKnownClientTypeEntry(
                typeof(LoggerProxy),"tcp://localhost:9090/LoggerProxy.rem");
            RemotingConfiguration.RegisterWellKnownClientType(remoteType);

            // Create a message sink.
            string objectUri;
            System.Runtime.Remoting.Messaging.IMessageSink messageSink = 
                clientChannel.CreateMessageSink(
                    "tcp://localhost:9090/LoggerProxy.rem", null,
                    out objectUri);
            Console.WriteLine("The URI of the message sink is {0}.", 
                objectUri);
            if (messageSink != null)
            {
                Console.WriteLine("The type of the message sink is {0}.", 
                    messageSink.GetType().ToString());
            }

            // Create an instance of the remote object.
            LoggerProxy service = new LoggerProxy();

            // Invoke a method on the remote object.
            Console.WriteLine("The client is invoking the remote object.");
            Console.WriteLine("The remote object has been called {0} times.",
                service.GetCount());

            while (true)
            {
                while (service.Count > 0)
                {
                    Console.WriteLine(service.Dequeue());
                }
            }
        }
    }
}