using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using Microsoft.Practices.EnterpriseLibrary.Data;

namespace Entero_Stock
{
    public static class Connection
    {

        static Database database;
        public static Database Database
        {
            get
            {
                if (database == null)
                {
                    //string db = "Data Source = 192.168.0.129; Initial Catalog = db_Entero_Stock; Integrated Security = True; Connect Timeout = 15; Encrypt = False; TrustServerCertificate = False; ApplicationIntent = ReadWrite; MultiSubnetFailover = False";
                    //database = DatabaseFactory.CreateDatabase("myDB");
                    SetDatabases();
                    database = DatabaseFactory.CreateDatabase("myDB");

                }
                return database;
            }
        }
        public static Database SetDatabases()
        {
                    
            return database;
        }
    }


}