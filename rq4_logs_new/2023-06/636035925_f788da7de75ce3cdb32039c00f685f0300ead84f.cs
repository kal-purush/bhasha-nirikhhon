namespace Cart.Extensions
{
    public static class HttpContextExtensions
    {
        public static string GetCorrelationId(this HttpContext context)
        {
            var correlationId = Guid.NewGuid().ToString();
            var isHeadersNotDefined = context?.Request?.Headers == null || !context.Request.Headers.Any();

            if (isHeadersNotDefined)
            {
                return correlationId;
            }

            var header = context.Request.Headers[Constants.Constants.Headers.CorrelationIdHeader];

            if (header.Count > 0)
            {
                correlationId = header[0];
            }

            context.Items[Constants.Constants.Headers.CorrelationIdHeader] = correlationId;

            return correlationId;
        }
    }
}