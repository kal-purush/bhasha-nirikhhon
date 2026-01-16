using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AptekaParsing
{
    public class LogWriter
    {
        
        private string path;
        public LogWriter(string filePath)
        {
            path = filePath;
        }


        public void LogCritical(string message)
        {
            Log(LogLevel.Critical, message);
        }
        public void LogCritical(Exception ex)
        {
            Log(LogLevel.Critical, ex);
        }
        public void LogInformation(string message)
        {
            Log(LogLevel.Information, message);
        }

        public void Log(LogLevel logLevel, string message, params object?[] args)
        {
            var levelMessage = getLevelMessage(logLevel);

            var logMessage = levelMessage + message;
            Console.WriteLine(logMessage);
            WriteToFile(logMessage);
        }
        public void Log(LogLevel logLevel, Exception? exception)
        {
            var levelMessage = getLevelMessage(logLevel);
            var logMessage = levelMessage + exception.Message;
            Console.WriteLine(logMessage);
            WriteToFile(logMessage);
            



        }


        private string getLevelMessage(LogLevel logLevel)
        {
            string levelMessage = logLevel switch
            {
                LogLevel.Critical => "Critical Exception: ",
                LogLevel.Information => "Information: ",
                _ => "Debug: "
            };
            return levelMessage;
        }
        private void WriteToFile(string message)
        {
            if (!File.Exists(path))
            {
                using (StreamWriter sw = File.CreateText(path))
                {
                    sw.WriteLine(message);
                }
                return;
            }

            using (StreamWriter sw = File.AppendText(path))
            {
                sw.WriteLine(message);
            }

        }


    }
}