using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DFC.App.JobProfile.RefreshSegments
{
    public class RefreshSegments
    {
        [FunctionName("RefreshSegments")]
        public void Run([TimerTrigger("0 0 * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }
    }
}