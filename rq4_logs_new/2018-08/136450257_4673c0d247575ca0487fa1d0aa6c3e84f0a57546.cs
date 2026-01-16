using System;
using System.Configuration;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace NCS.DSS.Goal.ServiceBus
{
    public static class ServiceBusClient
    {
        public static readonly string KeyName = ConfigurationManager.AppSettings["KeyName"];
        public static readonly string AccessKey = ConfigurationManager.AppSettings["AccessKey"];
        public static readonly string BaseAddress = ConfigurationManager.AppSettings["BaseAddress"];
        public static readonly string QueueName = ConfigurationManager.AppSettings["QueueName"];

        public static async Task SendPostMessageAsync(Models.Goal goal, string reqUrl)
        {
            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(KeyName, AccessKey);
            var messagingFactory = MessagingFactory.Create(BaseAddress, tokenProvider);
            var sender = messagingFactory.CreateMessageSender(QueueName);

            var messageModel = new MessageModel()
            {
                TitleMessage = "New Goal record {" + goal.GoalId + "} added at " + DateTime.UtcNow,
                CustomerGuid = goal.CustomerId,
                LastModifiedDate = goal.LastModifiedDate,
                URL = reqUrl,
                IsNewCustomer = false
            };

            var msg = new BrokeredMessage(messageModel)
            {
                ContentType = "application/json",
                MessageId = goal.CustomerId + " " + DateTime.UtcNow
            };

            //msg.ForcePersistence = true; Required when we save message to cosmos
            await sender.SendAsync(msg);
        }

        public static async Task SendPatchMessageAsync(Models.Goal goal, Guid customerId, string reqUrl)
        {
            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(KeyName, AccessKey);
            var messagingFactory = MessagingFactory.Create(BaseAddress, tokenProvider);
            var sender = messagingFactory.CreateMessageSender(QueueName);
            var messageModel = new MessageModel
            {
                TitleMessage = "Goal record modification for {" + customerId + "} at " + DateTime.UtcNow,
                CustomerGuid = customerId,
                LastModifiedDate = goal.LastModifiedDate,
                URL = reqUrl,
                IsNewCustomer = false
            };

            var msg = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(messageModel))))
            {
                ContentType = "application/json",
                MessageId = customerId + " " + DateTime.UtcNow
            };

            //msg.ForcePersistence = true; Required when we save message to cosmos
            await sender.SendAsync(msg);
        }

    }

    public class MessageModel
    {
        public string TitleMessage { get; set; }
        public Guid? CustomerGuid { get; set; }
        public DateTime? LastModifiedDate { get; set; }
        public string URL { get; set; }
        public bool IsNewCustomer { get; set; }
    }

}
