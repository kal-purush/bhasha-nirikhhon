using System;
using System.Reflection;
using ClientNexus.Domain.Exceptions.ServerErrorsExceptions;
using ClientNexus.Domain.Interfaces;
using Microsoft.Data.SqlClient;

namespace ClientNexus.Infrastructure.Repositories
{
    public class DbTryCatchDecorator<T> : DispatchProxy
        where T : class
    {
        protected T? _decorated;

        public static T Create(T decorated)
        {
            ArgumentNullException.ThrowIfNull(decorated);
            object proxy = Create<T, DbTryCatchDecorator<T>>();
            ((DbTryCatchDecorator<T>)proxy)._decorated = decorated;
            return (T)proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            try
            {
                var result = targetMethod?.Invoke(_decorated, args);
                if (result is Task task)
                {
                    return HandleAsync((dynamic)task);
                }

                return result;
            }
            catch (SqlException ex)
            {
                if (ex.Number == -2 || ex.Number == 0)
                {
                    throw new ConnectionEstablishmentException("Database connection failed.", ex);
                }
                else if (ex.Number == 258)
                {
                    throw new TimeoutException("Database operation timed out.", ex);
                }

                throw;
            }
        }

        private async Task HandleAsync(Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (SqlException ex) when (ex.Number == -2 || ex.Number == 0)
            {
                throw new ConnectionEstablishmentException("Database connection failed.", ex);
            }
            catch (SqlException ex) when (ex.Number == 258)
            {
                throw new TimeoutException("Database operation timed out.", ex);
            }
        }

        // Handle generic Task<T>
        private async Task<T1> HandleAsync<T1>(Task<T1> task)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (SqlException ex) when (ex.Number == -2 || ex.Number == 0)
            {
                throw new ConnectionEstablishmentException("Database connection failed.", ex);
            }
            catch (SqlException ex) when (ex.Number == 258)
            {
                throw new TimeoutException("Database operation timed out.", ex);
            }
        }
    }
}