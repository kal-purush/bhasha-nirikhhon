using Restauracja.Utilities;
using System;
using System.Collections.Generic;
using System.Data.Entity.Infrastructure.Interception;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Restauracja.Services
{
    public static class Logger
    {
        private static LogBase logger = null;
        public static void Log(LogTarget target, Exception exception)
        {
            switch (target)
            {
                case LogTarget.File:
                    logger = new FileLogger();
                    logger.Log(exception);
                    break;
                case LogTarget.EventLog:
                    logger = new EventLogger();
                    logger.Log(exception);
                    break;
                case LogTarget.Database:
                    logger = new DbLogger();
                    logger.Log(exception);
                    break;
                default:
                    return;
            }
        }
    }
}