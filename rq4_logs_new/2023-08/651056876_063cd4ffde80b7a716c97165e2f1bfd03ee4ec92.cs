using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Threading.Tasks;
using Vastra.API.Models.CustomException;

namespace Vastra.API.Middleware
{
    public class ExceptionHandlingMiddleware
    {
        public RequestDelegate requestDelegate;
        public ExceptionHandlingMiddleware(RequestDelegate requestDelegate)
        {
            this.requestDelegate = requestDelegate;
        }

        public async Task Invoke(HttpContext context, ILogger<ExceptionHandlingMiddleware> logger)
        {
            try
            {
                await requestDelegate(context);
            }
            catch (Exception ex)
            {
                await HandleException(context, ex, logger);
            }
        }

        private static Task HandleException(HttpContext context, Exception ex, ILogger<ExceptionHandlingMiddleware> logger)
        {
            //log exception
            logger.LogError(ex.ToString());

            var errorMessageObject = new Error { Message = ex.Message };
            var statusCode = (int)HttpStatusCode.InternalServerError;

            switch (ex)
            {
                case QuantityOutOfLimitException:
                    statusCode = (int)HttpStatusCode.BadRequest;
                    break;
                case UpdatePaidOrderException:
                    statusCode = (int)HttpStatusCode.BadRequest;
                    break;
            }
            var errorMessage = JsonConvert.SerializeObject(errorMessageObject);
            context.Response.ContentType = "application/json";
            context.Response.StatusCode = statusCode;

            return context.Response.WriteAsync(errorMessage);
        }
    }
}