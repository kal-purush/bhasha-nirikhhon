using System.Linq;

namespace Server.Adapters.Http
{
    internal class DebugResource : Resource
    {
        protected override (int status, string content) Get(IHttpRequest request)
        {
            var html = "<html><div><h2>Welcome to ES http server v0.1</h2></div>" +
                       "<br>" +
                       $"<div><h3>Original Request</h3>" +
                       $"<pre>{request}</pre></div>" +
                       $"<div><h3>PathParameters</h3>" +
                       $"<ul>{request.PathParameters.Aggregate("", (c, n) => c + $"<li>{n.Item1} : {n.Item2}</li>")}</ul>" +
                       "</div>" +
                       $"<div><h3>QueryParameters</h3>" +
                       $"<ul>{request.QueryParameters.Aggregate("", (c, n) => c + $"<li>{n.Item1} = {n.Item2}</li>")}</ul>" +
                       "</div>" + "</html>";

            return (200, html);
        }

        protected override (int status, string content) Post(IHttpRequest request)
            => MakeNotFound;

        protected override (int status, string content) Put(IHttpRequest request)
            => MakeNotFound;

        protected override (int status, string content) Delete(IHttpRequest request)
            => MakeNotFound;

        private static (int, string) MakeNotFound => (200, "Not Found");
    }
}