using System;
using Newtonsoft.Json;

namespace AppmetrCS.Serializations
{
    public class NewtonsoftSerializerTyped : IJsonSerializer
    {
        public static readonly NewtonsoftSerializerTyped Instance = new NewtonsoftSerializerTyped();
        
        private readonly JsonSerializerSettings _jsonSerializerSettings =
            new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Auto};
        
        public String Serialize(Object obj)
        {
            return JsonConvert.SerializeObject(obj, _jsonSerializerSettings);
        }

        public T Deserialize<T>(String json)
        {
            return JsonConvert.DeserializeObject<T>(json, _jsonSerializerSettings);
        }
    }
}