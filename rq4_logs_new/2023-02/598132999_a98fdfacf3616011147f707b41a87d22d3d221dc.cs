using System;
using System.Threading.Tasks;
using DataModel.Events;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BillingFunctions;

public static class OrdersPlacedHandler
{
    [FunctionName("OrdersPlacedHandler")]
    [return: ServiceBus("ordersbilled", Connection = "ServiceBusConnection")]
    public static async Task<string> AsyncRun([ServiceBusTrigger("ordersplaced", "Billing", Connection = "ServiceBusConnection")] string message, Int32 deliveryCount,
        DateTime enqueuedTimeUtc,
        string messageId,
        ILogger log)
    {
        log.LogInformation($"C# ServiceBus queue trigger function processed message: {JsonConvert.SerializeObject(message)}");
        log.LogInformation($"EnqueuedTimeUtc={enqueuedTimeUtc}");
        log.LogInformation($"DeliveryCount={deliveryCount}");
        log.LogInformation($"MessageId={messageId}");

        OrderPlaced orderPlaced = JsonConvert.DeserializeObject<OrderPlaced>(message);
        OrderBilled orderBilled = new OrderBilled() { OrderId = orderPlaced.OrderId };
        
        Console.WriteLine($"Created file: invoice-{orderPlaced.OrderId}.txt");

        return JsonConvert.SerializeObject(orderBilled);
    }
}