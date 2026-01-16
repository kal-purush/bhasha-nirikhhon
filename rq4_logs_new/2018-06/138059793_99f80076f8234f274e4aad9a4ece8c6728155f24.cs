using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Traffic.Models;

namespace Traffic.DAL
{
    public static class TrafficDb
    {
        private static string connectionString;
        private static TrafficContext context;

        public static TrafficContext getConnection()
        {
            connectionString = String.Format($"server={Properties.Settings.Default.ServerName};" +
               $"port={Properties.Settings.Default.PortNumber};" +
               $"database={Properties.Settings.Default.DatabaseName};" +
               $"uid={Properties.Settings.Default.UserName};" +
               $"password={Properties.Settings.Default.Password}");
            context = new TrafficContext(connectionString);
            return context;
        }

        public static bool checkConnection()
        {
            try
            {
                context.Database.Connection.Open();
                context.Database.Connection.Close();
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }
    }
}