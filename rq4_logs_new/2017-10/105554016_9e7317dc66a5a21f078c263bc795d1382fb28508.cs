using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Helpers.IHelpers;
using log4net;

namespace Common.Helpers
{
    public class Log4NetHelper : ILogger
    {
        private static readonly ILog logger = LogManager.GetLogger("MainLogger");

        public void LogError(string message, Exception ex)
        {
            logger.Error(message, ex);
        }

        public void LogInfo(string message)
        {
            logger.Info(message);
        }

        public void LogWarn(string message, Exception ex)
        {
            logger.Warn(message, ex);
        }
    }
}