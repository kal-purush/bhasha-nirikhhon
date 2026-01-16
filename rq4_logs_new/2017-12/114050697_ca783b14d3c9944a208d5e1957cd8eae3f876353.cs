using Twilio.TwiML;
using Twilio.TwiML.Messaging;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace JukeboxFunctions
{
    public static class ReplyToJukeboxRequest
    {
        [FunctionName("ReplyToJukeboxRequest")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            return GetTwilioResponse();
        }

        public static HttpResponseMessage GetTwilioResponse()
        {
            string body = "Thanks for sending your request. We will reply to your request shortly";

            var twilioResponse = new MessagingResponse();
            twilioResponse.Append(new Message(body));

            var httpResponseMessage = new HttpResponseMessage();
            httpResponseMessage.Content = new StringContent(twilioResponse.ToString());
            httpResponseMessage.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/xml");

            return httpResponseMessage;
        }
    }
}