using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace EST.MIT.InvoiceImporter;

public static class HealthCheck
{
    [FunctionName("healthy")]
    public static IActionResult RunHealthy(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req, ILogger log)
    {
        log.LogInformation("Healthy check.");

        return new StatusCodeResult((int)HttpStatusCode.OK);
    }

    [FunctionName("healthz")]
    public static IActionResult RunHealthz(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req, ILogger log)
    {
        log.LogInformation("Healthz check");

        return new StatusCodeResult((int)HttpStatusCode.OK);
    }
}