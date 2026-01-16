using Orangebeard.SpecFlowPlugin.ClientExecution.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orangebeard.SpecFlowPlugin.ClientExtensibility
{
    /// <summary>
    /// Extensibility point to bring ability to modify log message produced by tests.
    /// </summary>
    public interface ILogFormatter
    {
        /// <summary>
        /// Order of the formatter in chain of registered log message formatters.
        /// </summary>
        int Order { get; }

        /// <summary>
        /// Modify log message before sending it to the server.
        /// </summary>
        /// <param name="logMessage">Log message to format</param>
        /// <param name="newLogMessage">Formatted log message, if any</param>
        /// <param name="logMessageAttachment">Attachment to the log message, if the log message contained a file address.</param>
        /// <returns>Specify whether log message is formatted and should not be sent up to formatters chain.</returns>
        //bool FormatLog(CreateLogItemRequest logRequest);
        bool FormatLog(string logMessage, out string newLogMessage, out LogMessageAttachment logMessageAttachment);
    }
}