using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Http;
using System.Linq;

namespace Api.Middleware.Telemetry
{
    public class AttemptTelemetryInitialiser : ITelemetryInitializer
    {
        private IHttpContextAccessor httpContextAccessor;

        public AttemptTelemetryInitialiser(IHttpContextAccessor contextAccessor)
        {
            httpContextAccessor = contextAccessor;
        }

        public void Initialize(ITelemetry telemetry)
        {
            if (httpContextAccessor.HttpContext != null)
            {
                var attemptHeader = httpContextAccessor.HttpContext.Request.Headers["Attempt"];

                if (attemptHeader.Count() > 0)
                {
                    if (!telemetry.Context.Properties.ContainsKey("Attempt"))
                    {
                        telemetry.Context.Properties.Add("Attempt", attemptHeader);
                    }
                }
            }
        }
    }
}