using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;

using NLog;

namespace translate_spa.Controllers.ActionFilters
{
    public class GlobalLoggingExceptionFilter : IExceptionFilter
    {
        private readonly ILogger _log;
        public GlobalLoggingExceptionFilter(LogFactory loggerFactory)
        {
            _log = loggerFactory.GetCurrentClassLogger();
        }

        public void OnException(ExceptionContext context)
        {
            _log.Error(context.Exception.Message);
            _log.Error(context.Exception.StackTrace);
            context.HttpContext.Response.Headers.Add("X-Error-Message", context.Exception.Message);
            context.HttpContext.Response.StatusCode = StatusCodes.Status500InternalServerError;
        }

    }
}