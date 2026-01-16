using dbcontext.Classes;
using Newtonsoft.Json;

namespace Linktindr_be.Controllers {
    public class UsertypeEnumConverter : JsonConverter {

        public override bool CanConvert(Type objectType) {
            return objectType == typeof(UsertypeEnum);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) {
            if(reader.Value == null) {
                return null;
            }

            string value = reader.Value.ToString();

            if(string.IsNullOrEmpty(value)) {
                return null;
            }

            UsertypeEnum result;
            if(!Enum.TryParse(value, true, out result)) {
                throw new JsonSerializationException($"Invalid value '{value}' for UsertypeEnum");
            }

            return result;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) {
            throw new NotImplementedException();
        }
    }
}