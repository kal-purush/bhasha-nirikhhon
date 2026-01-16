using HTTPServer.core;

namespace HTTPServer.app
{
    public class ContentsCriteria : ICriteria
    {
        public bool ShouldRun(Request request)
        {
            return IsGetMethod(request.Method);
        }

        private bool IsGetMethod(string requestMethod)
        {
            return requestMethod.Equals("GET");
        }
    }
}