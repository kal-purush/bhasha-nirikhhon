using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace Lykke.Common.ApiLibrary.Middleware
{
    internal static class RequestUtils
    {
        public static async Task<string> GetRequestBodyAsync(HttpContext context)
        {
            // request body might be already read at the moment 
            if (context.Request.Body.CanSeek)
            {
                context.Request.Body.Seek(0, SeekOrigin.Begin);
            }

            using (var stream = new MemoryStream())
            {
                context.Request.Body.CopyTo(stream);

                stream.Seek(0, SeekOrigin.Begin);

                const int maxBodySize = 1024 * 64;
                var len = (int)Math.Min(stream.Length, maxBodySize);
                var bodyPart = new char[len];

                stream.Seek(0, SeekOrigin.Begin);

                using (var requestReader = new StreamReader(stream))
                {
                    await requestReader.ReadAsync(bodyPart, 0, len);

                    return new string(bodyPart);
                }
            }
        }

        public static string GetUrlWithoutQuery(string url)
        {
            var index = url.IndexOf('?');
            var urlWithoutQuery = index == -1 ? url : url.Substring(0, index);

            return urlWithoutQuery;
        }
    }
}