using MovieFiles.Core.Models.Activity;
using MovieFiles.Core.Models;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

public class BaseActivityConverter : JsonConverter
{
    public override bool CanConvert(Type objectType)
    {
        return (objectType == typeof(BaseActivity));
    }

    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        JObject jObject = JObject.Load(reader);

        BaseActivity result;
        switch (jObject["Type"].Value<string>())
        {
            case "RATED":
                result = new RatingActivity();
                break;
            // Add other types as needed...
            default:
                throw new ArgumentException($"Invalid activity type: {jObject["Type"].Value<string>()}");
        }

        serializer.Populate(jObject.CreateReader(), result);

        return result;
    }

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        throw new NotImplementedException(); // This won't be called as we're not serializing
    }
}