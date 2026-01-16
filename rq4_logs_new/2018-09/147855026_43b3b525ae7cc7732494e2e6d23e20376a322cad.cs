
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using System;
using Microsoft.Azure.Management.StreamAnalytics;
using System.Threading;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Rest;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Management.StreamAnalytics.Models;
using Microsoft.Rest.Azure;

namespace OptimiseAzureResources
{
    public static class StopAsa
    {
        [FunctionName("StopAsa")]
        public static IActionResult Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "stopjob")]
            HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var jobName = "-";

            try
            {
                string resourceGroupName = Environment.GetEnvironmentVariable("DefaultASAJobRG");
                string streamingJobName = Environment.GetEnvironmentVariable("DefaultASAJobName");

                var subscriptionId = Environment.GetEnvironmentVariable("SDK-SubscriptionId");

                SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

                // Get credentials
                ServiceClientCredentials credentials = GetCredentials();

                // Create Stream Analytics management client
                StreamAnalyticsManagementClient streamAnalyticsManagementClient = new StreamAnalyticsManagementClient(credentials)
                {
                    SubscriptionId = subscriptionId
                };

                var asaJobs = streamAnalyticsManagementClient.StreamingJobs.List();
                var asaJobList = new List<StreamingJob>(asaJobs);
                var myJob = asaJobList.Where(j => j.Name == streamingJobName).FirstOrDefault();

                if (myJob == null)
                {
                    var message = $"No Azure Stream Analytics Job found with name: {jobName}";

                    log.LogError($"{message} in subscription: {subscriptionId}");

                    return new BadRequestObjectResult(message );
                }


                if (myJob.JobState != "Running")
                {
                    var message = $"Azure Stream Analytics Job '{jobName}' is not running.";

                    log.LogError($"{message} - Job state is  {myJob.JobState}");

                    return new BadRequestObjectResult(message);
                }

                // Stop a streaming job
                streamAnalyticsManagementClient.StreamingJobs.Stop(resourceGroupName, streamingJobName);
            }
            catch (CloudException ex)
            {
                log.LogError($"Error occurred: {ex.ToString()}");
                return new BadRequestObjectResult("Error occured, check the logs on the server");
            }

            return (ActionResult)new OkObjectResult($"Job successfully stopped : {jobName}");
        }

        private static ServiceClientCredentials GetCredentials()
        {
            var clientId = Environment.GetEnvironmentVariable("SDK-ClientId");
            var clientSecret = Environment.GetEnvironmentVariable("SDK-ClientSecret");
            var tenantId = Environment.GetEnvironmentVariable("SDK-TenantId");
            
            var credentials = SdkContext.AzureCredentialsFactory
                .FromServicePrincipal(clientId,
                clientSecret,
                tenantId,
                AzureEnvironment.AzureGlobalCloud);

            return credentials;
        }
    }
}