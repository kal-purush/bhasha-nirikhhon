using Orangebeard.SpecFlowPlugin.ClientExtensibility.Commands;
using System;

namespace Orangebeard.SpecFlowPlugin.ClientExecution.Logging
{
    //TODO!~ Either remove the use of this altogether, or replace the dummy with [a variant of] the original ILogMessage implementation.
    class DummyLogMessage : ILogMessage
    {
        /// <summary>
        /// Textual log event message.
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Time representation when log event occurs.
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// Level of log event.
        /// </summary>
        public LogMessageLevel Level { get; set; }

        /// <summary>
        /// Binary data attached to log event.
        /// Null if log event is without attachment.
        /// </summary>
        public ILogMessageAttachment Attachment { get; set; }
    }

    abstract public class BaseLogScope : ILogScope
    {
        //TODO?+ protected IExtensionManager _extensionManager;

        protected ICommandsSource _commandsSource;

        //TODO?+ Add IExtensionManager and [I]CommandsSource?
        public BaseLogScope(ILogContext logContext /*, IExtensionManager extensionManager*/ /*, ICommandsSource commandsSource */)
        {
            Context = logContext;
            //TODO?+ _extensionManager = extensionManager;
            //TODO?+ _commandsSource = commandsSource;

            BeginTime = DateTime.UtcNow;
        }

        public virtual string Id { get; } = Guid.NewGuid().ToString();

        public virtual ILogScope Parent { get; }

        public virtual ILogScope Root { get; protected set; }

        public virtual ILogContext Context { get; }

        public virtual string Name { get; }

        public virtual DateTime BeginTime { get; }

        public virtual DateTime? EndTime { get; private set; }

        public virtual LogScopeStatus Status { get; set; } = LogScopeStatus.InProgress;

        public virtual ILogScope BeginScope(string name)
        {
            //TODO?+ Include extensionManager and _commandsSource?
            var logScope = new LogScope(Context /*, _extensionManager */ /*, _commandsSource*/, Root, this, name);

            //TODO?+ Context.Log = logScope;

            return logScope;
        }

        public void Debug(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Debug;
            Message(logMessage);
        }

        public void Debug(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Debug;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public void Error(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Error;
            Message(logMessage);
        }

        public void Error(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Error;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public void Fatal(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Fatal;
            Message(logMessage);
        }

        public void Fatal(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Fatal;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public void Info(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Info;
            Message(logMessage);
        }

        public void Info(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Info;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public void Trace(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Trace;
            Message(logMessage);
        }

        public void Trace(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Trace;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public void Warn(string message)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Warning;
            Message(logMessage);
        }

        public void Warn(string message, string mimeType, byte[] content)
        {
            var logMessage = GetDefaultLogRequest(message);
            logMessage.Level = LogMessageLevel.Warning;
            logMessage.Attachment = GetAttachFromContent(mimeType, content);
            Message(logMessage);
        }

        public virtual void Message(ILogMessage log)
        {
            //TODO?+ CommandsSource.RaiseOnLogMessageCommand(_commandsSource, Context, new Extensibility.Commands.CommandArgs.LogMessageCommandArgs(this, log));
        }

        protected ILogMessage GetDefaultLogRequest(string text)
        {
            //TODO?~
            var logMessage = new DummyLogMessage { Message = text, Attachment = null, Level = LogMessageLevel.Info /* ? */, Time = DateTime.Now };
            /*
            var logMessage = new LogMessage(text);
             */
            return logMessage;
        }

        protected ILogMessageAttachment GetAttachFromContent(string mimeType, byte[] content)
        {
            //TODO?+ return new LogMessageAttachment(mimeType, content);
            return null;
        }

        public virtual void Dispose()
        {
            EndTime = DateTime.UtcNow;

            if (Status == LogScopeStatus.InProgress)
            {
                Status = LogScopeStatus.Passed;
            }
        }
    }
}