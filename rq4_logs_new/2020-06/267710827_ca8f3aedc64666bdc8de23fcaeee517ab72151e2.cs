using Business;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Common.Cache;

namespace Data
{
    public class SesionDao:ConnectionToSql
    {
        public void createSesion(int Id_user, DateTime Date)
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                using (var command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.Parameters.AddWithValue("@Id_user", Id_user);
                    command.Parameters.AddWithValue("@Date", Date);
                    command.CommandText = "INSERT INTO REG_ACCESO VALUES (@Id_user,@Date);";
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                    //Obtener el numero de sesion
                    command.CommandText = "select * from REG_ACCESO where Id_usuario = @Id_user and  Fecha_acceso = @Date";
                    command.CommandType = CommandType.Text;
                    SqlDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            SesionCache.Id_acceso = reader.GetInt32(1);
                            Console.WriteLine("ID_acceso: " + SesionCache.Id_acceso);
                        }
                    }
                }
            }
        }

        public void create_reg_function(int Id_funcion, int Id_user, int Id_acceso)
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                using (var command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.Parameters.AddWithValue("@Id_funcion", Id_funcion);
                    command.Parameters.AddWithValue("@Id_user", Id_user);
                    command.Parameters.AddWithValue("@Id_acceso", Id_acceso);
                    command.CommandText = "INSERT INTO REG_FUNCION VALUES(@Id_funcion ,@Id_user ,@Id_acceso );";
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}