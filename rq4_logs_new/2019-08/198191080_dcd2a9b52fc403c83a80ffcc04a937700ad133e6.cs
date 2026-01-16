using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using NProducts.Data.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NProducts.Web.Filters
{
    /// <summary>
    /// Logs Action start/end. 
    /// Uses option, which on/off logging parameters of Action method (by default – it’s off).
    /// </summary>
    public class LogActionAttribute : IActionFilter
    {
        private ILogger<LogActionAttribute> logger;
        private NProductsOptions options;

        public LogActionAttribute(ILogger<LogActionAttribute> logger, IOptions<NProductsOptions> options)
        {
            this.logger = logger;
            this.options = options.Value;
        }

        public void OnActionExecuted(ActionExecutedContext context)
        {
            this.logger.LogInformation("Action [{url}] ended.", context.HttpContext.Request.Path);
        }

        public void OnActionExecuting(ActionExecutingContext context)
        {
            this.logger.LogInformation("Action [{url}] started.", context.HttpContext.Request.Path);

            if (options.LogActionParameters)
            {
                foreach(var key in context.ActionArguments.Keys)
                {                    
                    this.logger.LogInformation("Action [{url}] parameter [{key}] [{@params}].", context.HttpContext.Request.Path, key, JsonConvert.SerializeObject(context.ActionArguments[key]));
                }
            }
        }
    }
}