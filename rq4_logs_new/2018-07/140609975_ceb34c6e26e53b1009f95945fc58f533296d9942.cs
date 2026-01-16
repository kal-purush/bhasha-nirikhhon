using rydavidson.Accela.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rydavidson.Accela.Common
{
    public abstract class ExceptionHandler
    {
        private Logger logger;

        public void HandleException(Exception e)
        {
            HandleException(e, "");
        }
        public void HandleException(Exception e, string message)
        {
            HandleException(e, logger, message);
        }
        public void HandleException(Exception e, Logger _logger)
        {
            HandleException(e, _logger);
        }
        public void HandleException(Exception e, Logger _logger, string message)
        {
            LogException(e, _logger, message);
        }

        private void LogException(Exception e, string message)
        {
            LogException(e, logger, message);
        }
        private void LogException(Exception e, Logger _logger)
        {
            LogException(e, _logger, "");
        }
        private void LogException(Exception e, Logger _logger, string message)
        {
            SetLogger(_logger);

#if DEBUG
            Console.WriteLine(message);
            Console.WriteLine(e.Message);
            Console.WriteLine(e.StackTrace);
#endif
            if (!string.IsNullOrWhiteSpace(message))
                logger.Error(message);
            if (!string.IsNullOrWhiteSpace(e.Message))
                logger.Error(e.Message);
            if (!string.IsNullOrWhiteSpace(e.StackTrace))
                logger.Error(e.StackTrace);
        }

        public void SetLogger(Logger _logger)
        {
            logger = _logger;
        }

    }
}