namespace Errs.WebUi
{
    using System;
    using Serilog;
    using Serilog.Events;
    using SerilogWeb.Classic.Enrichers;

    public class LogConfig
    {
        private readonly IConfigurations _configurations;
        private readonly IConnectionStrings _connectionStrings;

        public LogConfig(IConnectionStrings connectionStrings, IConfigurations configurations)
        {
            _connectionStrings = connectionStrings;
            _configurations = configurations;
        }

        public void Initialize()
        {
            LogEventLevel logLevel;
            if (!Enum.TryParse(_configurations.LogLevel, out logLevel))
            {
                logLevel = LogEventLevel.Debug;
            }

            Log.Logger = new LoggerConfiguration()
                .WriteTo.Trace(outputTemplate: "{Timestamp} [{Level}] ({HttpRequestId}|{UserName}) {Message}{NewLine}{Exception}")
                .WriteTo.MSSqlServer(_connectionStrings.LoggingDb, _configurations.LogTableName, true, logLevel)
                .Enrich.With<HttpRequestIdEnricher>()
                .Enrich.With<HttpSessionIdEnricher>()
                .Enrich.With<HttpRequestUrlEnricher>()
                .Enrich.With(new UserNameEnricher())
                .CreateLogger();
        }
    }
}