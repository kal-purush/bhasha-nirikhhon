using Newtonsoft.Json.Linq;
using WebRequest.Elegant;

namespace EbSoft.Warehouse.SDK
{
    public class JObjectAsJsonBody : IJsonObject
    {
        private readonly JObject _jsonBody;

        public JObjectAsJsonBody(JObject jsonBody)
        {
            _jsonBody = jsonBody;
        }

        public string ToJson()
        {
            return _jsonBody.ToString();
        }
    }
}