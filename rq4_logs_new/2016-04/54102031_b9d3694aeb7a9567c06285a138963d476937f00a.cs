using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.OracleClient;

namespace ICT4Rails.Data
{
    class Database
    {
        private static string UserID = "dbi306801"; //Dit is de gebruikersnaam
        private static string Password = "kARHC2VPYo"; //Dit is het wachtwoord
        private static string DataSource = " //fhictora01.fhict.local:1521/fhictora";

        private static string connectionString = "User Id=" + UserID + ";Password=" + Password + ";Data Source=" + DataSource + ";";

        public static OracleConnection Connection
        {
            get
            {
                OracleConnection connection = new OracleConnection(connectionString);
                try
                {
                    connection.Open();
                }
                catch (Exception)
                {
                    MessageBox.Show("No Connection Established");
                }
                return connection;
            }
        }
    }
}