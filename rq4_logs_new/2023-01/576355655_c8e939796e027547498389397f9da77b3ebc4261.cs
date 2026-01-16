using System;
using System.IO;

namespace Boxinator_V2 {
    public static class Logger {
        private static int logFileNumber = 0;
        private static string logFileName = "LogFile_";
        private static string logFileExtension = ".txt";

        static Logger()
        {
            string logFilePath = logFileName + logFileNumber.ToString() + logFileExtension;
            while (File.Exists(logFilePath))
            {
                logFileNumber++;
                logFilePath = logFileName + logFileNumber.ToString() + logFileExtension;
            }
        }

        public static void Log(string message)
        {
            string logFilePath = logFileName + logFileNumber.ToString() + logFileExtension;
            using (StreamWriter logFile = new StreamWriter(logFilePath, true))
            {
                logFile.WriteLine(DateTime.Now.ToString() + " : " + message);
            }
        }
        
        // Debug log
        public static void LogDebug(string message)
        {
            #if DEBUG
            Log(message);
            #endif
        }
    }
}