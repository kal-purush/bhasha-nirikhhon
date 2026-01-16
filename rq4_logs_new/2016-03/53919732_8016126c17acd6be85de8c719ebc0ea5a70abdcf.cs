using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlHelperLibrary
{
    public interface ISqlHelper
    {
        public void Close();
        public int ExecuteNonQuery(string cmdText, CommandType cmdType, params SqlParameter[] cmdParameters);
        public SqlDataReader ExecuteReader(string cmdText, CommandType cmdType, params SqlParameter[] cmdParameters);
        public object ExecuteScalar(string cmdText, CommandType cmdType, params SqlParameter[] cmdParameters);
        void Open();
    }
}