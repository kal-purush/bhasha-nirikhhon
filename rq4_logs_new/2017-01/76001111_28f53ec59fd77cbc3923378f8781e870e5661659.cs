using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;

namespace ClinicManager.Common
{
    public class ConfigService
    {
        public static bool ChangeConnectionString(string serverName, string database, string username, string password)
        {
            try
            {
                string appPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                string configPath = System.IO.Path.Combine(appPath, "App.config");
                ExeConfigurationFileMap configFileMap = new ExeConfigurationFileMap();
                configFileMap.ExeConfigFilename = configPath;
              
                var configFile = ConfigurationManager.OpenMappedExeConfiguration(configFileMap, ConfigurationUserLevel.None);
                var setting = configFile.ConnectionStrings.ConnectionStrings;
                var connectString = setting["ClinicDB"].ConnectionString;

                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectString);
                builder.DataSource = serverName;
                builder.InitialCatalog = database;
                builder.UserID = username;
                builder.Password = password;

                setting["ClinicDB"].ConnectionString = builder.ConnectionString;
                
                configFile.Save(ConfigurationSaveMode.Modified);
                ConfigurationManager.RefreshSection(configFile.ConnectionStrings.SectionInformation.Name);
                return TryConnect();
                //return true;
            }
            catch (ConfigurationErrorsException)
            {
                return false;
            }
        }

        

        public static bool TryConnect()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["ClinicDB"].ConnectionString;

            var connect = new SqlConnection(connectionString);
            try
            {
                connect.Open();
                connect.Close();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool TryConnect(string serverName, string database, string username, string password)
        {
            string connectString = "Data Source=;Initial Catalog=;Integrated Security=True;Connection Timeout=3";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectString);
            builder.DataSource = serverName;
            builder.InitialCatalog = database;
            builder.UserID = username;
            builder.Password = password;

            string connectionString = builder.ConnectionString;
            var connect = new SqlConnection(connectionString);
            try
            {
                connect.Open();
                connect.Close();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static List<string> GetDatabase(string server, string userName, string password)
        {
            var databases = new List<string>();

            string connectString = "Data Source=;Initial Catalog=;Integrated Security=True;Connection Timeout=3";
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectString);

            builder.DataSource = server;
            builder.InitialCatalog = "master";
            if (userName != "")
            {
                builder.UserID = userName;
            }
            
            if(password != "")
            {
                builder.Password = password;
            }

            var sql = new SqlConnection(builder.ConnectionString);
            try
            {
                sql.Open();
                var reader = new SqlCommand("SP_DATABASES", sql).ExecuteReader();
                while (reader.Read())
                    databases.Add(reader[0].ToString());
                sql.Close();
            }
            catch (Exception)
            {
                // ignored
            }

            return databases;
        }
    }
}