using System;
using System.Collections.Generic;
using System.Text;
using Business;
using System.Data.SqlClient;
using Common.Cache;
using System.Data;

namespace Data
{
    public class RecuperaDao:ConnectionToSql
    {
        public bool VerificaDatos(string correo)
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                using (var command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.Parameters.AddWithValue("@correo", correo);
                    command.CommandText = "select * from USUARIO where CORREO = @correo";
                    command.CommandType = CommandType.Text;
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        Console.WriteLine("Datos correctos ");
                        while (reader.Read())
                        {
                            UserRecupera.nombre_completo = reader.GetString(2);
                            UserRecupera.nombre_completo += " "+reader.GetString(3);
                            UserRecupera.contrasenia = reader.GetString(5);
                        }
                        return true;
                    }
                    else
                    {
                        Console.WriteLine("Datos correctos ");
                        return false;
                    }
                }
            }
        }
    }
}