using System;

namespace Entities.Http
{
    internal static class ResourceExtension
    {
        public static IRouteHandler ToRouteHandler(this Resource resource)
            => new HttpRouteHandler(resource);
    }

    internal class HttpRouteHandler : IRouteHandler
    {
        private Resource _inner;

        internal HttpRouteHandler(Resource inner) => _inner = inner;

        public object Map(object input)
        {
            var request = input as IHttpRequest;
            if(request == null)
                throw new ArgumentException();

            return _inner.Map(request);
        }
    }
}