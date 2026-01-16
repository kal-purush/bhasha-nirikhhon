using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataConnect.Properties;
using Microsoft.EntityFrameworkCore;
using Wisend.Domain.Models;

namespace Wisend.Domain.DataSQL
{
    public class Database
    {
        public Database()
        {

        }

        public List<AccountWS> AllAcc { get; set; }
        public List<AccountWS> GetAccount()
        {
            string connectionString = VladBase.ConnectionString;
            string sqlExpression = Procedures.AddAccount;
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(sqlExpression, connection);
                // указываем, что команда представляет хранимую процедуру
                command.CommandType = System.Data.CommandType.StoredProcedure;
                // параметр для ввода имени
                SqlParameter nameParam = new SqlParameter
                {
                    ParameterName = "@Text",
                    Value = Console.ReadLine(),
                };
                // добавляем параметр
                command.Parameters.Add(nameParam);
                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    Console.WriteLine("{0}\t{1}\t{2}", reader.GetName(0), reader.GetName(1), reader.GetName(2));

                    while (reader.Read())
                    {
                        string Members = reader.GetString(0);
                        string Status = reader.GetString(1);
                        string Discipline = reader.GetString(2);
                    }
                }
            }
            return new List<AccountWS>();
        }
    }
}