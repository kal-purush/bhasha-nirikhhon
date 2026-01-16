using System;
using Azure.Storage.Files.Shares;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace AzureFunctions
{
    public class OrderItemsReserverMessage
    {
        [FunctionName("OrderItemsReserverMessage")]
        [FixedDelayRetry(3, "00:00:05")]
        public async Task Run([ServiceBusTrigger("orderitemsreserverqueue", Connection = "ServiceBusConnection")]string myQueueItem,
            ExecutionContext executionContext,
            ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
            log.LogInformation($"{executionContext?.InvocationId}. Retry: {executionContext?.RetryContext?.RetryCount} with RetryMax: {executionContext?.RetryContext?.MaxRetryCount}.");

            dynamic data = JsonConvert.DeserializeObject(myQueueItem);

            byte[] byteArray = Encoding.UTF8.GetBytes(myQueueItem);
            MemoryStream stream = new MemoryStream(byteArray);
            stream.Position = 0;

            var shareServiceClient = new ShareServiceClient("DefaultEndpointsProtocol=https;AccountName=storageapenkov;AccountKey=wJNnIkbX2mQ0vRHJH2EJpw1ryKbs8bmOA6WUKv0fuDx7cHKweysEQ3PFE6TxRn2l5MH3Jkdu/U8f+AStNy8toQ==;EndpointSuffix=core.windows.net");
            var shareClient = shareServiceClient.GetShareClient("order");
            var shareDirectoryClient = shareClient.GetRootDirectoryClient();

            var shareFileClient = await shareDirectoryClient.CreateFileAsync($"order-{data.ItemId}.json", stream.Length);
            var shareFileUploadInfo = shareFileClient.Value.Upload(stream);
            if (shareFileUploadInfo.GetRawResponse().IsError)
                throw new Exception("An error has occurred during file uploading");
        }
    }
}