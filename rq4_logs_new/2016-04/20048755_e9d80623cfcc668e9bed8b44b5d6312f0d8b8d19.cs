using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace xpf.Scripting.SQLServer
{
    /// <summary>
    /// Set of extension methods for SqlServer ADO.NET
    /// </summary>
    static class SqlServerExtensions
    {
        public static void AddInParameter(this SqlCommand command, string name, SqlDbType type, object value)
        {
            if (value == null)
                value = DBNull.Value;

            var parameter = command.Parameters.AddWithValue(name, value);
            parameter.SqlDbType = type;
        }

        public static void AddStructuredInParameter(this SqlCommand command, string name, SqlDbType type, IEnumerable value, string tableType)
        {
            object tvp = null;
            var hasRecords = value?.GetEnumerator()?.MoveNext();
            if (value != null && hasRecords.GetValueOrDefault())
                tvp = new TableValueCollection(value as IEnumerable<object>);

            var parameter = command.Parameters.AddWithValue(name, tvp);
            parameter.SqlDbType = type;
            parameter.TypeName = tableType;
        }
        public static void AddOutParameter(this SqlCommand command, string name, SqlDbType type)
        {
            var p = command.Parameters.Add(name, type, int.MaxValue);
            p.Direction = ParameterDirection.Output;
        }
    }
}