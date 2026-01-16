using System.Text;
using Newtonsoft.Json;

public static class HttpRequestDataExtensions
{
    public static void TryParseJson<TOutputType>(this Stream @this, out TOutputType? result)
    {
        using var streamReader = new StreamReader(@this, encoding: Encoding.UTF8);
        var json = streamReader.ReadToEnd();

        if (string.IsNullOrWhiteSpace(json))
        {
            result = default;
            throw new ArgumentException("Empty json");
        }

        try
        {
            result = JsonConvert.DeserializeObject<TOutputType>(json);
        }
        catch (Exception ex) when(ex is JsonSerializationException or JsonReaderException)
        {
            result = default;
            throw new ArgumentException(ex.Message);
        }
    }
}