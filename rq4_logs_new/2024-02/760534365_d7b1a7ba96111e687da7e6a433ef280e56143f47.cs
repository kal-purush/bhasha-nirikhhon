using Messaging.Buffer;
using Microsoft.AspNetCore.Mvc;
using WebAppExample.Requests;

namespace WebAppExample.Controllers
{
    [Route("app")]
    public class HelloWorldController : Controller
    {
        private readonly IServiceProvider _serviceProvider;

        public HelloWorldController(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            var messaging = _serviceProvider.GetRequiredService<IMessaging>();
            messaging.SubscribeRequestAsync<HelloWorldRequest>(OnHelloWorldRequestReceived);

        }

        [HttpGet("hello-world")]
        public async Task<string> RunHelloWorld()
        {
            var helloWorldBuffer = _serviceProvider.GetRequiredService<HelloWorldRequestBuffer>();
            var response = await helloWorldBuffer.SendRequestAsync();

            Console.WriteLine(response.InstanceResponse);

            return response.InstanceResponse;
        }

        private async void OnHelloWorldRequestReceived(string correlationId, HelloWorldRequest request)
        {
            var messaging = _serviceProvider.GetRequiredService<IMessaging>();
            await messaging.PublishResponseAsync(correlationId, new HelloWorldResponse(correlationId)
            {
                InstanceResponse = $"Hello from {Environment.MachineName}"
            });
        }
    }
}