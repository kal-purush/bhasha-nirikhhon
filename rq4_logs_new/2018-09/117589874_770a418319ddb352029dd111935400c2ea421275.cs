using Common.Log;
using System;
using System.Threading.Tasks;
using Lykke.Common.Log;
using Lykke.Job.PayTransactionHandler.Core;
using Lykke.Service.PayInternal.Client.Exceptions;

namespace Lykke.Job.PayTransactionHandler.Services.Extensions
{
    public static class LogExtensions
    {
        public static async Task<TResult> LogExceptionIfAny<TResult, TException>(this ILog log,
            Func<Task<TResult>> action, Func<TException, string> getExceptionDetails = null)
        {
            try
            {
                return await action();
            }
            catch (Exception e)
            {
                if (e is TException typedException)
                {
                    string message = getExceptionDetails?.Invoke(typedException) ?? e.ToDetails();

                    log.Error(e, message);
                }

                throw;
            }
        }

        public static async Task LogExceptionIfAny<TException>(this ILog log, Func<Task> action, Func<TException, string> getExceptionDetails = null)
        {
            try
            {
                await action();
            }
            catch (Exception e)
            {
                if (e is TException typedException)
                {
                    string message = getExceptionDetails?.Invoke(typedException) ?? e.ToDetails();

                    log.Error(e, message);
                }

                throw;
            }
        }

        public static Task LogPayInternalExceptionIfAny(this ILog log, Func<Task> action)
        {
            return log.LogExceptionIfAny<DefaultErrorResponseException>(action, ex => ex.Error.ToDetails());
        }

        public static Task<T> LogPayInternalExceptionIfAny<T>(this ILog log, Func<Task<T>> action)
        {
            return log.LogExceptionIfAny<T, DefaultErrorResponseException>(action, ex => ex.Error.ToDetails());
        }
    }
}