using System;
using System.IO;
using log4net;

namespace Log4NetLibrary
{
    public enum LogLevel
    {
        Debug = 1,
        Error,
        Fatal,
        Info,
        Warn
    }

    public sealed class FileLogService : ILogService
    {
        readonly ILog _logger;

        static FileLogService()
        {
            var log4NetConfigDirectory = AppDomain.CurrentDomain.RelativeSearchPath ?? AppDomain.CurrentDomain.BaseDirectory;

            var log4NetConfigFilePath = Path.Combine(log4NetConfigDirectory, "log4net.config");
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo(log4NetConfigFilePath));

        }

        public FileLogService()
        {
            _logger = LogManager.GetLogger(typeof(FileLogService));
        }

        #region Methods

        public void WriteLog(LogLevel logLevel, String log)
        {
            if (logLevel.Equals(LogLevel.Debug))
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug(log);
            }

            else if (logLevel.Equals(LogLevel.Error))
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error(log);
            }

            else if (logLevel.Equals(LogLevel.Fatal))
            {
                if (_logger.IsFatalEnabled)
                    _logger.Fatal(log);
            }

            else if (logLevel.Equals(LogLevel.Info))
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info(log);
            }

            else if (logLevel.Equals(LogLevel.Warn))
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn(log);
            }

        }

        #endregion
    }
}