using System;

namespace Errs.WebUi.Infrastructure.Logging
{
    public interface ILogger
    {
        ILogger ForContext(string propertyName, object value, bool destructureObjects = false);

        ILogger ForContext<TSource>();

        ILogger ForContext(Type source);

        void Verbose(string messageTemplate, params object[] propertyValues);

        void Verbose(Exception exception, string messageTemplate, params object[] propertyValues);

        void Debug(string messageTemplate, params object[] propertyValues);

        void Debug(Exception exception, string messageTemplate, params object[] propertyValues);

        void Information(string messageTemplate, params object[] propertyValues);

        void Information(Exception exception, string messageTemplate, params object[] propertyValues);

        void Warning(string messageTemplate, params object[] propertyValues);

        void Warning(Exception exception, string messageTemplate, params object[] propertyValues);

        void Error(string messageTemplate, params object[] propertyValues);

        void Error(Exception exception, string messageTemplate, params object[] propertyValues);

        void Fatal(string messageTemplate, params object[] propertyValues);

        void Fatal(Exception exception, string messageTemplate, params object[] propertyValues);
    }
}