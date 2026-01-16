using ClasesBase;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vistas
{
    internal class TrabajarUsuario
    {
        public static DataTable list_roles()
        {
            SqlConnection cnn = new SqlConnection(Vistas.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "SELECT * FROM Rol";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;

            SqlDataAdapter da = new SqlDataAdapter(cmd);

            DataTable dt = new DataTable();
            da.Fill(dt);

            return dt;
        }

        public static void insert_users(Usuario user)
        {
            SqlConnection cnn = new SqlConnection(Vistas.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "INSERT INTO Usuario(Usu_NombreUsuario, Usu_ApellidoNombre, Usu_Contraseña, Rol_Codigo) values(@nombreUsuario, @apellidoNombre, @password, @rol)";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;

            cmd.Parameters.AddWithValue("@nombreUsuario", user.Usu_NombreUsuario);
            cmd.Parameters.AddWithValue("@apellidoNombre", user.Usu_ApellidoNombre);
            cmd.Parameters.AddWithValue("@password", user.Usu_Contrasenia);
            cmd.Parameters.AddWithValue("@rol", user.Rol_Codigo);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
        }
        public static void delete_user(string nombreUsuario)
        {
            SqlConnection cnn = new SqlConnection(Vistas.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "DELETE FROM Usuario WHERE Usu_NombreUsuario = @nombreUsuario";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;

            cmd.Parameters.AddWithValue("@nombreUsuario", nombreUsuario);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
        }

        public static void update_user(Usuario user)
        {
            SqlConnection cnn = new SqlConnection(Vistas.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "UPDATE Usuario SET Usu_NombreUsuario = @nombreUsuario, Usu_ApellidoNombre = @apellidoNombre, Usu_Contraseña = @password, Rol_Codigo = @rol WHERE Usu_ID = @id";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;

            cmd.Parameters.AddWithValue("@nombreUsuario", user.Usu_NombreUsuario);
            cmd.Parameters.AddWithValue("@apellidoNombre", user.Usu_ApellidoNombre);
            cmd.Parameters.AddWithValue("@password", user.Usu_Contrasenia);
            cmd.Parameters.AddWithValue("@rol", user.Rol_Codigo);
            cmd.Parameters.AddWithValue("@id", user.Usu_ID);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
        }

        public static Usuario GetUserById(int userId)
        {
            Usuario user = null;
            using (SqlConnection cnn = new SqlConnection(Vistas.Properties.Settings.Default.comdepConnectionString))
            {
                SqlCommand cmd = new SqlCommand("SELECT Usu_ID, Usu_NombreUsuario, Usu_ApellidoNombre, Usu_Contraseña, Rol_Codigo FROM Usuario WHERE Usu_ID = @id", cnn);
                cmd.Parameters.AddWithValue("@id", userId);
                cnn.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        user = new Usuario
                        {
                            Usu_ID = reader.GetInt32(reader.GetOrdinal("Usu_ID")),
                            Usu_NombreUsuario = reader.GetString(reader.GetOrdinal("Usu_NombreUsuario")),
                            Usu_ApellidoNombre = reader.GetString(reader.GetOrdinal("Usu_ApellidoNombre")),
                            Usu_Contrasenia = reader.GetString(reader.GetOrdinal("Usu_Contraseña")),
                            Rol_Codigo = reader.GetInt32(reader.GetOrdinal("Rol_Codigo"))
                        };
                    }
                }
            }
            return user;
        }
    }
}