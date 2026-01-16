using System.Net.Http;
using System.Threading.Tasks;

namespace httpclient_messagehandler_example.MessageHandler
{
    public class FancyMessageHandler : DelegatingHandler 
    {
        private int _count = 0;

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, System.Threading.CancellationToken cancellationToken)
        {
            System.Threading.Interlocked.Increment(ref _count);
            request.Headers.Add("X-Custom-Header", _count.ToString());
            
            return base.SendAsync(request, cancellationToken);
        }
    }
}