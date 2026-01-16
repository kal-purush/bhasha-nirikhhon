using System.Data.SqlClient;
using System.Data;

namespace Vodamep.Api.Engines.SqlServer
{

    public static class SqlDataReaderExtensions
    {
        public static T Get<T>(this SqlDataReader reader, string columnName)
        {
            if (reader.IsDBNull(columnName))
                return default;
            return reader.GetFieldValue<T>(columnName);
        }
    }
}