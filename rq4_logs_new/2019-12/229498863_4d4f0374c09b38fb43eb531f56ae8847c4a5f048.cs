using System;
using System.Collections.Generic;
using System.Text;

namespace FormsMvvm2019
{
    /// <summary>
    /// ロガー.
    /// ILoggerとして公開する.
    /// NLog実装に依存する部分はここで吸収する.
    /// </summary>
    public class AppLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly NLog.ILogger logger;

        /// <summary>
        /// GetLoggerで使用するコンストラクタ.
        /// </summary>
        /// <param name="name"></param>
        private AppLogger(string name)
        {
            logger = NLog.LogManager.GetLogger(name);
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel)
        {
            throw new NotImplementedException();
        }

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            logger.Log(NLogLevel[logLevel], formatter(state, exception));
        }

        /// <summary>
        /// ロガー取得.
        /// staticクラス用.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static Microsoft.Extensions.Logging.ILogger GetLogger(string name)
        {
            return new AppLogger(name);
        }

        /// <summary>
        /// ロガー取得.
        /// クラスインスタンス用.
        /// </summary>
        /// <param name="instance"></param>
        /// <returns></returns>
        public static Microsoft.Extensions.Logging.ILogger GetLogger(object instance)
        {
            return new AppLogger(instance.GetType().Name);
        }

        /// <summary>
        /// ロガー取得.
        /// 型指定用.
        /// なぜかnameofが通用しないためTになってしまう.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Microsoft.Extensions.Logging.ILogger GetLogger<T>()
        {
            return new AppLogger(nameof(T));
        }

        /// <summary>
        /// ログレベル変換辞書.
        /// </summary>
        private static Dictionary<Microsoft.Extensions.Logging.LogLevel, NLog.LogLevel> NLogLevel = new Dictionary<Microsoft.Extensions.Logging.LogLevel, NLog.LogLevel>
        {
            { Microsoft.Extensions.Logging.LogLevel.Trace, NLog.LogLevel.Trace },
            { Microsoft.Extensions.Logging.LogLevel.Debug, NLog.LogLevel.Debug },
            { Microsoft.Extensions.Logging.LogLevel.Information, NLog.LogLevel.Info },
            { Microsoft.Extensions.Logging.LogLevel.Warning, NLog.LogLevel.Warn },
            { Microsoft.Extensions.Logging.LogLevel.Error, NLog.LogLevel.Error },
            { Microsoft.Extensions.Logging.LogLevel.Critical, NLog.LogLevel.Fatal },
            { Microsoft.Extensions.Logging.LogLevel.None, NLog.LogLevel.Off },
        };

        /// <summary>
        /// ログ機能の初期化.
        /// </summary>
        public static void InitializeLog()
        {
            var config = new NLog.Config.LoggingConfiguration();

            // Targets where to log to: File and Console
            var fileName = System.IO.Path.Combine(Xamarin.Essentials.FileSystem.AppDataDirectory, "file.txt");
            System.Diagnostics.Debug.WriteLine($"---- fileName ---- {fileName}");
            var logfile = new NLog.Targets.FileTarget("logfile") { FileName = fileName };
            var logconsole = new NLog.Targets.ConsoleTarget("logconsole");

            // Rules for mapping loggers to targets
            // https://github.com/NLog/NLog/wiki/Configuration-file#log-levels
            config.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, logconsole);
            config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logfile);

            // Apply config           
            NLog.LogManager.Configuration = config;
        }
    }
}