using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using MySql.Data.MySqlClient;

using JoyOI.Monitor.Models;
using JoyOI.Monitor.Lib;

namespace JoyOI.Monitor.Controllers
{
    public class GraphController : BaseController
    {

        [NonAction]
        public async Task<Graph> GetGraphData(
            string name,
            string datasource,
            string sql,
            GraphScaling scale,
            Func<List<Dictionary<string, object>>, Graph> proc_rows
        )
        {
            List<Dictionary<string, object>> query_data = new List<Dictionary<string, object>>();
            using (var conn = new MySqlConnection(Startup.Config[datasource + ":ConnectionString"]))
            {
                await conn.OpenAsync();
                using (var cmd = new MySqlCommand(
                    sql + " LIMIT 0," + scale.Points, conn))
                {
                    cmd.Parameters.Add(new MySqlParameter("interval", scale.Interval));
                    using (var dr = await cmd.ExecuteReaderAsync())
                    {
                        while (await dr.ReadAsync()) {
                            var row = new Dictionary<string, object>();
                            for (var i = 0; i < dr.FieldCount; i++)
                            {
                                row.Add(dr.GetName(i), dr.GetValue(i));
                            }
                            query_data.Add(row);
                        }
                    }
                }
            }
            return proc_rows(query_data);
        }
    }
}