using MovieFiles.Api.Client;
using Polly;
using System;
using System.Net;
using System.Threading.Tasks;

public static class RetryHelper
{
    public static async Task<T> RetryOnExceptionAsync<T>(int retries, Func<Task<T>> operation)
    {
        return await Policy
            .Handle<ApiException>(e => e.StatusCode == (int)HttpStatusCode.InternalServerError ||
                                       e.StatusCode == (int)HttpStatusCode.BadGateway)
            .WaitAndRetryAsync(retries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)))
            .ExecuteAsync(operation);
    }
}