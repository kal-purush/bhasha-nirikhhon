using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TIIDE.Compile
{
    public class SqliteDataAccess
    {
        /// <summary>
        /// This function takes a single argument of type "integer".
        /// The integer given corresponds to the Token in the DB.
        /// </summary>
        /// <param name="tokenInteger"></param>
        /// <returns>A list element.</returns>
        public static List<TokenModel> FindTokenByInteger(int tokenInteger = 0)
        {
            string sqlFindTokenByInteger = "select * from Tokens where Integer is " + tokenInteger.ToString();

            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                var output = cnn.Query<TokenModel>(sqlFindTokenByInteger, new DynamicParameters());
                return output.ToList();
            }
        }

        /// <summary>
        /// This function takes a single argument of type "string".
        /// The string given corresponds to the Token in the DB.
        /// </summary>
        /// <param name="tokenString"></param>
        /// <returns>A list element.</returns>
        public static List<TokenModel> FindTokenByString(string tokenString = "")
        {
            string sqlFindTokenByString = "select * from Tokens where String is \"" + tokenString + "\"";

            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                var output = cnn.Query<TokenModel>(sqlFindTokenByString, new DynamicParameters());
                return output.ToList();
            }
        }

        /// <summary>
        /// This function takes two arguements of type "string".
        /// The strings given correspond to the hex values of the Token in the DB.
        /// </summary>
        /// <param name="HighByte">This is the first byte.</param>
        /// <param name="LowByte">This is the second byte.</param>
        /// <returns>A list element.</returns>
        public static List<TokenModel> FindTokenByHex(string HighByte, string LowByte)
        {
            string sqlFindTokenByHex = "select * from Tokens where HighByte = \"" + HighByte + "\" and LowByte = \"" + LowByte + "\"";
            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                var output = cnn.Query<TokenModel>(sqlFindTokenByHex, new DynamicParameters());
                return output.ToList();
            }
        }

        private static string LoadConnectionString(string id = "Default")
        {
            return ConfigurationManager.ConnectionStrings[id].ConnectionString;
        }
    }
}