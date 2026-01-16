using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NLogFlake.Constants;
using NLogFlake.Helpers;
using NLogFlake.Services;

namespace NLogFlake;

public static class IApplicationBuilderExtensions
{
    public static void ConfigureLogFlakeExceptionHandler(this IApplicationBuilder app, IConfiguration configuration)
    {
        app.UseExceptionHandler(applicationBuilder => applicationBuilder.Run(async httpContext => await ConfigureLogFlakeExceptionHandler(httpContext)));
    }

    private static async Task ConfigureLogFlakeExceptionHandler(HttpContext httpContext)
    {
        httpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
        httpContext.Response.ContentType = "application/json";

        IExceptionHandlerFeature contextFeature = httpContext.Features.Get<IExceptionHandlerFeature>();
        if (contextFeature is not null)
        {
            ILogFlakeService logFlakeService = httpContext.RequestServices.GetRequiredService<ILogFlakeService>();

            if (logFlakeService.Settings.AutoLogGlobalExceptions && contextFeature.Error is not OperationCanceledException)
            {
                ICorrelationService correlationService = httpContext.RequestServices.GetRequiredService<ICorrelationService>();
                string correlation = correlationService.Correlation;

                string? client = HttpContextHelper.GetClientId(httpContext);

                string logMessage = LogFlakeMiddlewareHelper.GetLogExceptionMessage(client, correlation);

                Dictionary<string, object> parameters = await HttpContextHelper.GetLogParametersAsync(httpContext, false);
                logFlakeService.WriteLog(LogLevels.ERROR, correlation, logMessage, parameters);
                logFlakeService.WriteException(contextFeature.Error!, correlation);
            }

            httpContext.Items[HttpContextConstants.HasCatchedError] = new();

            await httpContext.Response.WriteAsync(JsonSerializer.Serialize(new
            {
                Error = new
                {
                    ErrorCode = (int)HttpStatusCode.InternalServerError,
                    ErrorMessage = contextFeature.Error?.Message,
                },
                RequestStatus = "KO",
            }), CancellationToken.None);
        }
    }
}