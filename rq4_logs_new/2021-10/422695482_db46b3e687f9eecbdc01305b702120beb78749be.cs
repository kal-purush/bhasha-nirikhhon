using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace OmazonWebAPI.Connections
{
    public class _MySqlConnection
    {
        public MySqlConnection MySqlConnection { get; set; }
        public MySqlCommand MySqlCommand { get; set; }
        public MySqlDataReader MySqlDataReader { get; set; }

        public _MySqlConnection(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void InitMySqlComponents(string commandText)
        {
            this.MySqlConnection = new MySqlConnection();
            this.MySqlConnection.ConnectionString = Configuration.GetConnectionString("DB_Connection_Provider2");
            this.MySqlCommand = new MySqlCommand(commandText, this.MySqlConnection);
        }

        public void CreateParameter(string parameterName, MySqlDbType dbType, object value)
        {
            MySqlParameter sqlParameter = new(parameterName, dbType);
            sqlParameter.Value = value;
            this.MySqlCommand.Parameters.Add(sqlParameter);
        }

        public void CreateParameterOutput()
        {
            MySqlParameter ParameterReturn = new MySqlParameter();
            ParameterReturn.Direction = ParameterDirection.ReturnValue;
            this.MySqlCommand.Parameters.Add(ParameterReturn);
        }

        public void ExecuteConnectionCommands()
        {
            this.MySqlConnection.Open();
            this.MySqlCommand.CommandType = CommandType.StoredProcedure;
        }

        public void ExecuteNonQuery()
        {
            this.ExecuteConnectionCommands();
            this.MySqlCommand.ExecuteNonQuery();
            this.MySqlConnection.Close();
        }

        public void ExcecuteReader()
        {
            this.ExecuteConnectionCommands();
            this.MySqlDataReader = this.MySqlCommand.ExecuteReader();
        }
    }
}