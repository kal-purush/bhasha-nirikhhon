using System;
using System.Linq;
using System.Text;
using Entities;

namespace Server.Adapters.Http
{
    internal static class HttpMessageParser
    {
        public static IHttpRequest Parse(byte[] bytes)
        {
            var message = Encoding.ASCII.GetString(bytes);
            var extract = message.Substring(0, Math.Min(message.Length, 500));
            Debug.Write(
                $"Parsing raw request (showing max 500 characters):" +
                $"\r\n{extract}");

            var lines = message.Split("\r\n");

            if (lines.Length == 0)
                return new HttpRequest(null);

            (HttpVerb verb, string uri, string version) = ParseRquestLine(lines[0]);
            var headers = lines.Skip(1)
                .TakeWhile(line => line.Length > 0)
                .Select(MessageHeader.From);

            var body = lines
                .SkipWhile(line => line.Length > 0)
                .Aggregate((cur, next) => cur + next);

            return new HttpRequest(null)
            {
                Verb = verb,
                RequestUri = uri,
                HttpVersion = version,
                Headers = headers.ToArray(),
                Body = body
            };
        }

        private static (HttpVerb, string, string) ParseRquestLine(string s)
        {
            if (string.IsNullOrEmpty(s))
                throw new ArgumentException(nameof(s));

            var parts = s.Split(' ');

            return (
                (HttpVerb) Enum.Parse(typeof(HttpVerb), parts[0], true),
                parts.Length > 1 ? parts[1] : string.Empty,
                parts.Length > 2 ? parts[2] : string.Empty
                );
        }
    }
}