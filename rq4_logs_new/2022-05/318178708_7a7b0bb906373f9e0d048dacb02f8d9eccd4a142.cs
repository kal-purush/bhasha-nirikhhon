using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace XTI_Core;

public sealed class NumericValueJsonConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) => typeof(NumericValue).IsAssignableFrom(typeToConvert);

    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options) =>
        (JsonConverter)Activator.CreateInstance
        (
            typeof(NumericValueJsonConverter<>).MakeGenericType(new[] { typeToConvert }),
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            args: new object[0],
            culture: null
        )!;
}