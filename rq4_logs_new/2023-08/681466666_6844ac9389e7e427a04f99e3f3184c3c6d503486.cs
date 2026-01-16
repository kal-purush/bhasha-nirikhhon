// Ignore Spelling: Strapi Deserialize

using System.Globalization;

namespace BetterCoding.Strapi.SDK.Core.Services
{
    public interface IDataDecoder
    {
        object Decode(object data, IServiceHub serviceHub);
    }

    public class DataDecode : IDataDecoder
    {
        public object Decode(object data, IServiceHub serviceHub) => data switch
        {
            null => default,
            IDictionary<string, object> { } dictionary => dictionary.ToDictionary(pair => pair.Key, pair => Decode(pair.Value, serviceHub)),
            IList<object> { } list => list.Select(item => Decode(item, serviceHub)).ToList(),
            _ => data
        };

        internal static string[] DateFormatStrings { get; } =
        {
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'",
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'ff'Z'",
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'f'Z'",
        };

        public static DateTime ParseDate(string input) => DateTime.ParseExact(input, DateFormatStrings, CultureInfo.InvariantCulture, DateTimeStyles.None);
    }
}