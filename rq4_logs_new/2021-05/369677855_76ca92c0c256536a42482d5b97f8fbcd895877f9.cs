using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace AtmosAQ.Web.Configurations
{
    public class ExceptionHandlerConfig
    {
        private readonly RequestDelegate _requestDelegate;

        public ExceptionHandlerConfig(RequestDelegate requestDelegate)
        {
            _requestDelegate = requestDelegate;
        }

        public async Task Invoke(HttpContext context)
        {
            try
            {
                await _requestDelegate.Invoke(context);
            }
            catch (Exception ex)
            {
                await HandleExceptionMessageAsync(context, ex).ConfigureAwait(false);
            }
        }

        private static Task HandleExceptionMessageAsync(HttpContext context, Exception ex)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            response.StatusCode = (int)HttpStatusCode.InternalServerError;

            var errorResponse = new
            {
                message = ex.Message,
                statusCode = response.StatusCode
            };

            var json = JsonSerializer.Serialize(errorResponse);

            return response.WriteAsync(json);
        }

    }
}