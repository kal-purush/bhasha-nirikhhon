//-----------------------------------------------------------------------------------
// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
//-----------------------------------------------------------------------------------

using Azure.Messaging.ServiceBus;
        //[EventGridOutput(TopicEndpointUri = "MyEventGridTopicUriSetting", TopicKeySetting = "MyEventGridTopicKeySetting")]
        public async Task Run([EventGridTrigger] ImageEvent input, FunctionContext context)

            // ------------------Capture Event and create logs-----------------------------
            logger.LogInformation("Event Recieved...................!");

            // ------------------Convert Event Data Json to object and get image name-----------------------------

            var eventData = JsonConvert.DeserializeObject<EventData>(input.Data.ToString());


            // ------------------Send messages to Azure Service Bus-----------------------------
            var serviceBusConnectionString = "Endpoint=sb://vv00imgservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManagedSharedAccessKeys;SharedAccessKey=ImDqRVwvvGJL8cU1QQVaHVjeBTO5zicE8+ASbHe0UR4=;EntityPath=vv00imgqueue";

            // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
            await using var client = new ServiceBusClient(serviceBusConnectionString);

            // create the sender
            ServiceBusSender sender = client.CreateSender(queueName);

            // create a message that we can send. UTF-8 encoding is used when providing a string.
            var messageValue = $"{DateTime.Now}:New Image Uploaded - Image Name:  {imageUploaded} - Image Type: {eventData.contentType}";

            // send the message
            await sender.SendMessageAsync(message);