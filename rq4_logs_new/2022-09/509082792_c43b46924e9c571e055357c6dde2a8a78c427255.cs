using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;

namespace EntityDatabaseFirst
{
    public class ConectDB
    {
        [Benchmark]
        public void Run()
        {
            var connectionString = "Server=SVS-N-MP223FRX; Initial Catalog=Blog; User ID=sa; Password=S@to0310";
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "Select * From tbl_Post";
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var title = reader["Title"].ToString();
                        var context = reader["Context"].ToString();

                        Console.WriteLine("{0}=={1}", title, context);
                    }
                }
            }
        }
    }
}