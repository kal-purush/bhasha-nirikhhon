using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Security;

namespace SQLObjectBackup
{    
    public class SqlConnectGUI
    {

        private IEnumerable<SqlGetDatabases> _databases;


        public string SqlServerName { get; private set; }        
        public bool IsWindowsAuth { get; private set; }
        public string SqlUserName { get; private set; }
        public string Password { get; private set; }
        public IEnumerable<SqlGetDatabases> Databases { get { return GetDatabases(); } private set { _databases = value; } }

        /// <summary>
        /// windows auth constructor for GUI 
        /// </summary>
        /// <param name="sqlServerName"></param>
        public SqlConnectGUI(string sqlServerName)
        {
            SqlServerName = sqlServerName;
            IsWindowsAuth = true;            
        }      

        /// <summary>
        /// sql auth for GUI
        /// </summary>
        /// <param name="sqlServerName"></param>
        /// <param name="databaseName"></param>
        /// <param name="sqlUserName"></param>
        /// <param name="password"></param>
        public SqlConnectGUI(string sqlServerName, string sqlUserName, string password)
        {
            SqlServerName = sqlServerName;            
            IsWindowsAuth = false;
            SqlUserName = sqlUserName;
            Password = password;
        }

        private string GetConnectionString()
        {
            string connectionString = string.Format("data source = {0}; initial catalog = Master; ", SqlServerName);

            connectionString += IsWindowsAuth ? "trusted_connection = true;" : string.Format("user id = {0}; password = {1};", SqlUserName, Password);

            return connectionString;
        }
        public bool TestConnection()
        {
            using (SqlConnection databaseConnection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    databaseConnection.Open();
                    //databaseConnection.Close();
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public IEnumerable<SqlGetDatabases> GetDatabases()
        {
            DataTable output = new DataTable();
            using (SqlConnection databaseConnection = new SqlConnection(GetConnectionString()))
            using (SqlCommand sqlCmd = new SqlCommand())
            using (SqlDataAdapter sda = new SqlDataAdapter(sqlCmd))
            {
                sqlCmd.Connection = databaseConnection;
                sqlCmd.CommandText =
                    @"select database_id,
                             name 
                      from sys.databases
                      where database_id > 4;";
                sda.Fill(output);

                foreach (DataRow row in output.Rows)
                    yield return new SqlGetDatabases(
                        Convert.ToInt32(row["database_id"]),
                        row["name"].ToString());
            }
        }
    }
}