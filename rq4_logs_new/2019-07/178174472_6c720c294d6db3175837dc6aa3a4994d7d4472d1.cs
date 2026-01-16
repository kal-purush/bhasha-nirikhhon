using System;
using MononokeEngine.Logging;


namespace MononokeEngine.Toolset.Logging
{
    public class ConsoleLoggerServer : ILogger
    {
        
        private readonly ConsoleLoggerProxy _proxy;
        
        internal ConsoleLoggerServer()
        {
            Mononoke.Toolset.TCP.RegisterWellKnownServiceType(typeof(ConsoleLoggerProxy), "LoggerProxy.rem");
            _proxy = (ConsoleLoggerProxy) Activator.GetObject(typeof(ConsoleLoggerProxy), "tcp://localhost:9090/LoggerProxy.rem");
        }
        
        
        public void Print(string msg)
        {
            _proxy.Enqueue(msg);
        }
    }
}