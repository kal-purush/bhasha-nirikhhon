using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core.Logging;
using NLog;
using ILogger = Grpc.Core.Logging.ILogger;
using LogLevel = NLog.LogLevel;

namespace Cida.Server.Console
{
    public class GrpcLogger : ILogger
    {
        private readonly NLog.ILogger logger;
        private readonly Type forType;
        private readonly string typePrefix;

        public GrpcLogger(NLog.ILogger logger)
            : this(logger, null)
        {
        }

        public GrpcLogger(NLog.ILogger logger, Type forType)
        {
            this.forType = forType;
            this.logger = logger;
            if (this.forType != null)
            {
                this.typePrefix = $"[{this.forType.FullName}]";
            }
            else
            {
                this.typePrefix = string.Empty;
            }
        }

        public ILogger ForType<T>()
        {
            if (typeof(T) == this.forType)
            {
                return this;
            }
            
            return new GrpcLogger(this.logger, typeof(T));
        }

        /// <summary>Logs a message with severity Debug.</summary>
        public void Debug(string message)
        {
            this.logger.Log(LogLevel.Debug, message);
        }

        /// <summary>Logs a formatted message with severity Debug.</summary>
        public void Debug(string format, params object[] formatArgs)
        {
            this.Debug(string.Format(format, formatArgs));
        }

        /// <summary>Logs a message with severity Info.</summary>
        public void Info(string message)
        {
            this.logger.Info(message);
        }

        /// <summary>Logs a formatted message with severity Info.</summary>
        public void Info(string format, params object[] formatArgs)
        {
            this.Info(string.Format(format, formatArgs));
        }

        /// <summary>Logs a message with severity Warning.</summary>
        public void Warning(string message)
        {
            this.logger.Warn(message);
        }

        /// <summary>Logs a formatted message with severity Warning.</summary>
        public void Warning(string format, params object[] formatArgs)
        {
            this.Warning(string.Format(format, formatArgs));
        }

        /// <summary>Logs a message and an associated exception with severity Warning.</summary>
        public void Warning(Exception exception, string message)
        {
            this.logger.Warn(message, exception);
        }

        /// <summary>Logs a message with severity Error.</summary>
        public void Error(string message)
        {
            this.logger.Error(message);
        }

        /// <summary>Logs a formatted message with severity Error.</summary>
        public void Error(string format, params object[] formatArgs)
        {
            this.Error(string.Format(format, formatArgs));
        }

        /// <summary>Logs a message and an associated exception with severity Error.</summary>
        public void Error(Exception exception, string message)
        {
            this.logger.Error(message, exception);
        }
    }
}