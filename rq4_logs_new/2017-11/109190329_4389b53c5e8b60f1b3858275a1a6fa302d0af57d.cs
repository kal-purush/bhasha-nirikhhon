using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CRUDApp.Tools;

namespace CRUDApp.Adapter
{
    public class DbAdapter : IDbAdapter
    {
        public IDbCommand DbCommand { get; private set; }

        public IDbConnection DbConnection { get; private set; }

        private int _cmdTimeout = 5000;

        public int CommandTimeout
        {
            get { return _cmdTimeout; }
            set { _cmdTimeout = value; }
        }

        public DbAdapter(IDbCommand dbCommand, IDbConnection dbConnection)
        {
            DbCommand = dbCommand;
            DbConnection = dbConnection;
        }

        public IEnumerable<T> LoadObject<T>(IDbCmdDef cmdDef) where T : class
        {
            try
            {
                if (cmdDef == null)
                    throw new ArgumentException("Missing command definition");

                List<T> itms = new List<T>();

                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.CommandType = cmdDef.DbCommandType;
                    cmd.CommandText = cmdDef.DbCommandText;

                    cmd.Connection = conn;

                    if (cmdDef.DbParameters != null)
                        foreach (IDbDataParameter param in cmdDef.DbParameters)
                            cmd.Parameters.Add(param);

                    IDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        itms.Add(DataMapper<T>.Instance.MapToObject(reader));
                    }
                    return itms;
                }
            }
            catch
            {
                throw;
            }
        }

        public IEnumerable<T> LoadObject<T>(IDbCmdDef cmdDef, Func<IDataReader, T> mapper) where T : class
        {
            try
            {
                if (cmdDef == null)
                    throw new ArgumentException("Missing command definition");

                List<T> itms = new List<T>();

                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.CommandType = cmdDef.DbCommandType;
                    cmd.CommandText = cmdDef.DbCommandText;

                    cmd.Connection = conn;

                    if (cmdDef.DbParameters != null)
                        foreach (IDbDataParameter param in cmdDef.DbParameters)
                            cmd.Parameters.Add(param);

                    IDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        itms.Add(mapper(reader));
                    }
                    return itms;
                }
            }
            catch
            {
                throw;
            }
        }

        public int ExecuteQuery(IDbCmdDef cmdDef, Action<IDataParameterCollection> returnParameters = null)
        {
            try
            {
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.CommandType = cmdDef.DbCommandType;
                    cmd.CommandText = cmdDef.DbCommandText;
                    cmd.Connection = conn;

                    if (cmdDef.DbParameters != null)
                        foreach (IDbDataParameter param in cmdDef.DbParameters)
                            cmd.Parameters.Add(param);

                    int returnVal = cmd.ExecuteNonQuery();

                    returnParameters?.Invoke(cmd.Parameters);

                    return returnVal;
                }
            }
            catch
            {
                throw;
            }
        }

        public object ExecuteScalar(IDbCmdDef cmdDef)
        {
            try
            {
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.CommandType = cmdDef.DbCommandType;
                    cmd.CommandText = cmdDef.DbCommandText;
                    cmd.Connection = conn;

                    if(cmdDef.DbParameters != null)
                        foreach (IDbDataParameter param in cmdDef.DbParameters)
                            cmd.Parameters.Add(param);

                    return cmd.ExecuteScalar();
                }
            }
            catch
            {
                throw;
            }
        }
    }
}