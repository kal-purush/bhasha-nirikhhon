using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace OmazonWebAPI.Connections
{
    public class PostgreSQL
    {

        public NpgsqlCommand    NpgsqlCommand { get; set; }
        public NpgsqlConnection NpgsqlConnection { get; set; }
        public NpgsqlDataReader NpgsqlReader { get; set; }
        public NpgsqlParameter ParameterReturn { get; set; }
        
        public IConfiguration Configuration { get; }

        public void InitPostgresSqlComponents(string commandText)
        {
            this.NpgsqlConnection = new NpgsqlConnection(Configuration.GetConnectionString("DB_Connection_Provider3"));
            this.NpgsqlCommand = new NpgsqlCommand(commandText, this.NpgsqlConnection);
        }

        public void CreateParameter(string parameterName, NpgsqlDbType  dbType, object value)
        {
            NpgsqlParameter sqlParameter = new(parameterName, dbType);
            sqlParameter.Value = value;
            this.NpgsqlCommand.Parameters.Add(sqlParameter);
            //hola raque
        }

        public void CreateParameterOutput()
        {
            this.ParameterReturn = new NpgsqlParameter();
            ParameterReturn.Direction = ParameterDirection.ReturnValue;
            this.NpgsqlCommand.Parameters.Add(this.ParameterReturn);
        }

        public void ExecuteNonQuery()
        {
            this.ExecuteConnectionCommands();
            this.NpgsqlCommand.ExecuteNonQuery();
            this.NpgsqlConnection.Close();
        }

        public void ExcecuteReader()
        {
            this.ExecuteConnectionCommands();
            this.NpgsqlReader = this.NpgsqlCommand.ExecuteReader();
        }

        public void ExecuteConnectionCommands()
        {
            this.NpgsqlConnection.Open();
            this.NpgsqlCommand.CommandType = CommandType.StoredProcedure;
        }
    }
}