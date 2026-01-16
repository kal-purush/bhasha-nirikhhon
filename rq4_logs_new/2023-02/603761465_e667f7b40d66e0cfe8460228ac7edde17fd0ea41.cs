using System;
using Microsoft.EntityFrameworkCore;

namespace WebAPIServer.Models
{
    /// <summary>
    /// MySQLと接続するためのコンテキスト
    /// https://learn.microsoft.com/ja-jp/ef/core/dbcontext-configuration/
    /// </summary>
	public class MySQLContext:DbContext
	{

        public static string Host { get; set; }
        public static string Port { get; set; }
        public static string Pass { get; set; }
        public static string User { get; set; }
        public static string DBName { get; set; }


        static MySQLContext()
        {
            Host = "localhost";
            Port = "3306";
            Pass = "rDcrmPRLJvu@ex/E,>K";
            User = "forest";
            DBName = "";
        }

        public MySQLContext(string db)
        {
            DBName = db;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            var connectionString = $"server='{Host}';port='{Port}';user='{User}';password='{Pass}';Database='{DBName}'";

            var serverVersion = new MySqlServerVersion(new Version(8, 0, 30));

            optionsBuilder.UseMySql(connectionString, serverVersion);
        }
    }
}
