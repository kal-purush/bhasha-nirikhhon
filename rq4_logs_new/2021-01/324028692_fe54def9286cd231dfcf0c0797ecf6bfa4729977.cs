using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using RandomizerLib.Logging;

namespace MultiWorldServer
{
    class Logger : ILogger
    {
        public void Log(string message)
        {
            Server.Log($"[INFO] {message}");
        }

        public void Log(object message)
        {
            Server.Log($"[INFO] {message}");
        }

        public void LogDebug(string message)
        {
            Server.Log($"[DEBUG] {message}");
        }

        public void LogDebug(object message)
        {
            Server.Log($"[DEBUG] {message}");
        }

        public void LogError(string message)
        {
            Server.Log($"[ERROR] {message}");
        }

        public void LogError(object message)
        {
            Server.Log($"[ERROR] {message}");
        }

        public void LogFine(string message)
        {
            Server.Log($"[FINE] {message}");
        }

        public void LogFine(object message)
        {
            Server.Log($"[FINE] {message}");
        }

        public void LogWarn(string message)
        {
            Server.Log($"[WARN] {message}");
        }

        public void LogWarn(object message)
        {
            Server.Log($"[WARN] {message}");
        }
    }
}