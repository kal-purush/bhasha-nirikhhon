using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace OmazonWebAPI.Connections
{
    public class SqlServerConnection
    {
        public SqlCommand SqlCommand { get; set; }
        public SqlConnection SqlConnection { get; set; }
        public SqlDataReader SqlDataReader { get; set; }
        public SqlParameter ParameterReturn { get; set;}

        public SqlServerConnection(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

      

        public void InitSqlComponents(string commandText)
        {
            this.SqlConnection = new SqlConnection(Configuration.GetConnectionString("DB_Connection_Provider1"));
            this.SqlCommand = new SqlCommand(commandText, this.SqlConnection);
        }

        public void CreateParameter(string parameterName, SqlDbType dbType, object value)
        {
            SqlParameter sqlParameter = new(parameterName, dbType);
            sqlParameter.Value = value;
            this.SqlCommand.Parameters.Add(sqlParameter);
            //hola raque
        }

        public void CreateParameterOutput()
        {
            this.ParameterReturn = new SqlParameter();
            ParameterReturn.Direction = ParameterDirection.ReturnValue;
            this.SqlCommand.Parameters.Add(this.ParameterReturn);
        }

        public void ExecuteNonQuery()
        {
            this.ExecuteConnectionCommands();
            this.SqlCommand.ExecuteNonQuery();
            this.SqlConnection.Close();
        }

        public void ExcecuteReader()
        {
            this.ExecuteConnectionCommands();
            this.SqlDataReader = this.SqlCommand.ExecuteReader();
        }

        public void ExecuteConnectionCommands()
        {
            this.SqlConnection.Open();
            this.SqlCommand.CommandType = CommandType.StoredProcedure;
        }
    }
}