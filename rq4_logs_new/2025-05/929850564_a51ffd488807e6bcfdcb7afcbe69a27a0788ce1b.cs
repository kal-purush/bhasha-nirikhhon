using System.Reflection;
using ClientNexus.Domain.Exceptions.ServerErrorsExceptions;
using StackExchange.Redis;

namespace ClientNexus.Infrastructure
{
    public class CacheTryCatchDecorator<T> : DispatchProxy
        where T : class
    {
        protected T? _decorated;

        public static T Create(T decorated)
        {
            ArgumentNullException.ThrowIfNull(decorated);
            object proxy = Create<T, CacheTryCatchDecorator<T>>();
            ((CacheTryCatchDecorator<T>)proxy)._decorated = decorated;
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
            catch (RedisConnectionException ex)
            {
                throw new ConnectionEstablishmentException("Cache connection failed.", ex);
            }
            catch (RedisTimeoutException ex)
            {
                throw new TimeoutException("Cache operation timed out.", ex);
            }
        }

        private async Task HandleAsync(Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (RedisConnectionException ex)
            {
                throw new ConnectionEstablishmentException("Cache connection failed.", ex);
            }
            catch (RedisTimeoutException ex)
            {
                throw new TimeoutException("Cache operation timed out.", ex);
            }
        }

        // Handle generic Task<T>
        private async Task<T1> HandleAsync<T1>(Task<T1> task)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (RedisConnectionException ex)
            {
                throw new ConnectionEstablishmentException("Cache connection failed.", ex);
            }
            catch (RedisTimeoutException ex)
            {
                throw new TimeoutException("Cache operation timed out.", ex);
            }
        }
    }
}