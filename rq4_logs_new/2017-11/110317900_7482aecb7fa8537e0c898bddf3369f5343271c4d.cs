using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rup
{
    public
    static class DBA
    {
        private static readonly string SqlConnect = string.Format("Data Source={0};uid={1};pwd={2};database={3};Packet Size=4096;Pooling=true;Max Pool Size=512;Min Pool Size=1", new object[] { "103.200.23.85", "rupt02", "rupt02!@#", "RUP" });

        private static SqlCommand CreateCommandSql(SqlConnection conn, string procName, SqlParameter[] prams)
        {
            SqlCommand command = new SqlCommand(procName, conn) { CommandType = CommandType.Text, CommandTimeout = 180 };
            if (prams != null)
            {
                foreach (SqlParameter parameter in prams)
                {
                    command.Parameters.Add(parameter);
                }
            }
            command.Parameters.Add(new SqlParameter("ReturnValue", SqlDbType.Int, 4, ParameterDirection.ReturnValue, false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }

        public static DataTable GetDbToDataTable(string sqlCommand)
        {
            DataTable table;
            using (SqlConnection connection = new SqlConnection(SqlConnect))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter())
                {
                    using (adapter.SelectCommand = new SqlCommand(sqlCommand, connection))
                    {
                        try
                        {
                            connection.Open();
                        }
                        catch (Exception)
                        {
                            return null;
                        }
                        DataTable dataTable = new DataTable();
                        try
                        {
                            adapter.Fill(dataTable);
                        }
                        catch (Exception)
                        {
                            return null;
                        }
                        adapter.Dispose();
                        connection.Close();
                        connection.Dispose();
                        table = dataTable;
                    }
                }
            }
            return table;
        }

        public static DataTable GetDbToDataTable(string sqlCommand, SqlParameter[] prams)
        {
            DataTable table;
            using (SqlConnection connection = new SqlConnection(SqlConnect))
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter())
                {
                    using (adapter.SelectCommand = CreateCommandSql(connection, sqlCommand, prams))
                    {
                        try
                        {
                            connection.Open();
                        }
                        catch (Exception)
                        {
                            return null;
                        }
                        DataTable dataTable = new DataTable();
                        try
                        {
                            adapter.Fill(dataTable);
                        }
                        catch (Exception)
                        {
                            return null;
                        }
                        adapter.Dispose();
                        connection.Close();
                        connection.Dispose();
                        table = dataTable;
                    }
                }
            }
            return table;
        }

        public static int ExeSqlCommand(string sqlCommand)
        {
            int num2;
            using (SqlConnection connection = new SqlConnection(SqlConnect))
            {
                using (SqlCommand command = new SqlCommand(sqlCommand, connection))
                {
                    int num;
                    try
                    {
                        connection.Open();
                    }
                    catch
                    {
                        return -1;
                    }
                    try
                    {
                        num = command.ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                        return -1;
                    }
                    command.Dispose();
                    connection.Close();
                    connection.Dispose();
                    num2 = num;
                }
            }
            return num2;
        }
    }
}