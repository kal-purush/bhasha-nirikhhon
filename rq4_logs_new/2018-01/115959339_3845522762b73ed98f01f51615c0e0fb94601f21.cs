using Entities.Http;

namespace Server.Adapters.Http
{
    internal class ResourceController : Resource
    {
        private Resource _inner;
        public ResourceController(Resource inner)
        {
            _inner = inner;
        }

        public override (int status, string content) Delete(IHttpRequest request)
        => _inner.Delete(request);

        public override (int status, string content) Get(IHttpRequest request)
        => _inner.Get(request);

        public override (int status, string content) Post(IHttpRequest request)
        => _inner.Post(request);

        public override (int status, string content) Put(IHttpRequest request)
        => _inner.Put(request);

        internal IHttpResponse GetResponse(IHttpRequest request)
        {
            if (request == null)
                return new HttpResponse(HttpStatusCode.BadRequest);

            switch (request.Method)
            {
                case "GET":
                    return Make(Get(request));
                case "POST":
                    return Make(Post(request));
                case "PUT":
                    return Make(Put(request));
                case "DELETE":
                    return Make(Delete(request));
                default:
                    return new HttpResponse(HttpStatusCode.BadRequest);
            }
        }

        private IHttpResponse Make((int status, string content) result)
        {
            var response = new HttpResponse(HttpStatusCode.From(result.status));
            response.SetContent(result.content);

            return response;
        }
    }
}