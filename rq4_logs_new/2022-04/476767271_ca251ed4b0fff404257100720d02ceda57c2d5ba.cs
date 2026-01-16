using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Bcp.Api.Converters
{
    public class StringOutputFormatter : ITypeConverter
    {
        public object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
        {
            text = text.Replace(text.Substring(0, 11), "");
            return text;
        }

        public string ConvertToString(object value, IWriterRow row, MemberMapData memberMapData)
        {
            var retValue = value as string;
            retValue = retValue.Replace(retValue.Substring(0, 11), "");
            return retValue;
        }
    }
}