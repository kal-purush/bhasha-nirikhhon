using Microsoft.AspNetCore.Http;
using System;
using System.Net.Mime;
using System.Text.Json;
using System.Threading.Tasks;

namespace DB.Api.Middlewares
{
    internal class ApiExceptionMiddleware
    {
        private readonly RequestDelegate _next;

        public ApiExceptionMiddleware(RequestDelegate next) =>
            _next = next ?? throw new ArgumentNullException(nameof(next));

        public async Task InvokeAsync(HttpContext context)
        {
            try
            {
                await _next(context);
            }
            catch (Exception ex)
            {
                await HandleApiExceptionAsync(context, ex);
            }
        }

        private Task HandleApiExceptionAsync(HttpContext context, Exception exception)
        {
            ApiProblemDetails apiProblemDetails;
            switch (exception)
            {
                case TimeoutException tex:
                    context.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                    apiProblemDetails = new ApiProblemDetails(context, tex);
                    break;

                case NotImplementedException nie:
                    context.Response.StatusCode = StatusCodes.Status501NotImplemented;
                    apiProblemDetails = new ApiProblemDetails(context, nie);
                    break;

                default:
                    context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                    apiProblemDetails = new ApiProblemDetails(context, exception);
                    break;
            }

            context.Response.ContentType = MediaTypeNames.Application.Json;
            var response = JsonSerializer.Serialize(apiProblemDetails, new JsonSerializerOptions
            {
                IgnoreNullValues = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            return context.Response.WriteAsync(response);
        }
    }
}