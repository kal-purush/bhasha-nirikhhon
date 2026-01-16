using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Messaging;
using ServiceStack.RabbitMq;

namespace Livedoon.Api
{
    [Route("/api/md5")]
    // [Route("/api/md5?url={Url}")]
    public class HashGenerator : IReturn<HashResponse>
    {
        public string Url { get; set; }
        public string Subscriber { get; set; }
    }

    public class HashResponse
    {
        public bool Succeed { get; set; }
        public string Result { get; set; }
        public string Subscriber { get; set; }
        public string Url { get; set; }
    }

    public class HashServices : Service
    {
        private readonly IServerEvents _serverEvents;

        public HashServices(IServerEvents serverEvents)
        {
            if (serverEvents == null) throw new ArgumentNullException(nameof(serverEvents));
            _serverEvents = serverEvents;
        }
        public object Any(HashGenerator request)
        {
            // Check if client id exists.
            if (request.Subscriber == null)
                throw new HttpError(HttpStatusCode.BadRequest, $"The request is not valid - subscriber is empty."); ;

            var url = request.Url;
            // Check if the url is valid.
            // Since we don't really "use" the uri object, just create one inline.
            var urlValidationResult = Uri.TryCreate(url, UriKind.Absolute, out var uriResult) &&
                         (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);

            if (!urlValidationResult)
                throw new HttpError(HttpStatusCode.BadRequest, $"The url '{url}' is not valid."); ;

            var mqServer = new RabbitMqServer("localhost");

            // dispatch a new thread for message queue processing

            new Thread(() =>
            {
                using (var mqClient = mqServer.CreateMessageQueueClient())
                {   
                    mqClient.Publish(request);
                    var response = mqClient.Get<HashResponse>(QueueNames<HashResponse>.In);
                    mqClient.Ack(response);
                    // if there are any notification registered,
                    // push notification
                    _serverEvents.NotifyChannel("hash", response.Body);
                }
            }).Start();

            return null;
        }
    }
}