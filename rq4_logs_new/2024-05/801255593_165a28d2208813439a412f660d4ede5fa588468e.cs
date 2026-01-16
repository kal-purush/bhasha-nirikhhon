using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClasesBase
{
    public class TrabajoAtleta
    {
        public static void agregarAtleta(Atleta atleta)
        {
            SqlConnection cnn = new SqlConnection(ClasesBase.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "INSERT INTO Atleta(" +
                "Alt_DNI, Alt_Apellido, Alt_Nombre, Alt_Nacionalidad, Alt_Entrenador, " +
                "Alt_Genero, Alt_Altura, Alt_Peso, Alt_FechaNac, Alt_Direccion, Alt_Email) " +
                "values(@Dni, @Apellido, @Nombre, @Nacionalidad, @Entrenador, @Genero, " +
                "@Altura, @Peso, @FechaNac, @Direccion, @Email)";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;
            cmd.Parameters.AddWithValue("@Dni", atleta.Atl_DNI);
            cmd.Parameters.AddWithValue("@Apellido", atleta.Atl_Apellido);
            cmd.Parameters.AddWithValue("@Nombre", atleta.Atl_Nombre);
            cmd.Parameters.AddWithValue("@Nacionalidad", atleta.Atl_Nacionalidad);
            cmd.Parameters.AddWithValue("@Entrenador", atleta.Atl_Entrenador);
            cmd.Parameters.AddWithValue("@Genero", atleta.Atl_Genero);
            cmd.Parameters.AddWithValue("@Altura", atleta.Atl_Altura);
            cmd.Parameters.AddWithValue("@Peso", atleta.Atl_Peso);
            cmd.Parameters.AddWithValue("@FechaNac", atleta.Atl_FechaNac);
            cmd.Parameters.AddWithValue("@Direccion", atleta.Atl_Direccion);
            cmd.Parameters.AddWithValue("@Email", atleta.Atl_Email);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
        }
        public static Atleta obtenerAtletaById(Int32 id)
        {
            Atleta oAtleta = null;
            using (SqlConnection cnn = new SqlConnection(ClasesBase.Properties.Settings.Default.comdepConnectionString))
            {
                SqlCommand cmd = new SqlCommand("SELECT Alt_ID, Alt_DNI, Alt_Apellido, Alt_Nombre, " +
                    "Alt_Nacionalidad, Alt_Entrenador, Alt_Genero, Alt_Altura, Alt_Peso, " +
                    "Alt_FechaNac, Alt_Direccion, Alt_Email FROM Atleta WHERE Alt_ID = @id", cnn);
                cmd.Parameters.AddWithValue("@id", id);
                cnn.Open();
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        oAtleta = new Atleta(reader.GetInt32(reader.GetOrdinal("Alt_ID")),
                            reader.GetString(reader.GetOrdinal("Alt_DNI")),
                            reader.GetString(reader.GetOrdinal("Alt_Apellido")),
                            reader.GetString(reader.GetOrdinal("Alt_Nombre")),
                            reader.GetString(reader.GetOrdinal("Alt_Nacionalidad")),
                            reader.GetString(reader.GetOrdinal("Alt_Entrenador")),
                            reader.GetString(reader.GetOrdinal("Alt_Genero"))[0],
                            reader.GetDouble(reader.GetOrdinal("Alt_Altura")),
                            reader.GetDouble(reader.GetOrdinal("Alt_Peso")),
                            reader.GetDateTime(reader.GetOrdinal("Alt_FechaNac")),
                            reader.GetString(reader.GetOrdinal("Alt_Direccion")),
                            reader.GetString(reader.GetOrdinal("Alt_Email"))
                            );
                    }
                }
                cnn.Close();
            }
            return oAtleta;
        }

        public static void borrarAtleta(Int32 id)
        {
            SqlConnection cnn = new SqlConnection(ClasesBase.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "DELETE FROM Atleta WHERE Alt_ID = @id";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;
            cmd.Parameters.AddWithValue("@id", id);

            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();

        }
        public static void ModificarAtleta(Atleta atleta)
        {
            SqlConnection cnn = new SqlConnection(ClasesBase.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "UPDATE Atleta SET Alt_DNI = @dni, " +
            "Alt_Apellido = @apellido, " +
            "Alt_Nombre = @nombre, " +
            "Alt_Nacionalidad = @nacionalidad, " +
            "Alt_Entrenador = @entrenador, " +
            "Alt_Genero = @genero, " +
            "Alt_Altura = @altura, " +
            "Alt_Peso = @peso, " +
            "Alt_FechaNac = @fechaNac, " +
            "Alt_Direccion = @direccion, " +
            "Alt_Email = @email " +
            "WHERE Alt_ID = @id";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = cnn;

            cmd.Parameters.AddWithValue("@dni", atleta.Atl_DNI);
            cmd.Parameters.AddWithValue("@apellido", atleta.Atl_Apellido);
            cmd.Parameters.AddWithValue("@nombre", atleta.Atl_Nombre);
            cmd.Parameters.AddWithValue("@nacionalidad", atleta.Atl_Nacionalidad);
            cmd.Parameters.AddWithValue("@entrenador", atleta.Atl_Entrenador);
            cmd.Parameters.AddWithValue("@genero", atleta.Atl_Genero);
            cmd.Parameters.AddWithValue("@altura", atleta.Atl_Altura);
            cmd.Parameters.AddWithValue("@peso", atleta.Atl_Peso);
            cmd.Parameters.AddWithValue("@fechaNac", atleta.Atl_FechaNac);
            cmd.Parameters.AddWithValue("@direccion", atleta.Atl_Direccion);
            cmd.Parameters.AddWithValue("@email", atleta.Atl_Entrenador);
            cmd.Parameters.AddWithValue("@id", atleta.Atl_ID);
            cnn.Open();
            cmd.ExecuteNonQuery();
            cnn.Close();
        }

        public static List<Atleta> obtenerAtletas()
        {
            SqlConnection cnn = new SqlConnection(ClasesBase.Properties.Settings.Default.comdepConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "SELECT * FROM Atleta";
            cmd.Connection = cnn;

            cnn.Open();
            List<Atleta> atletas = new List<Atleta>();
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Atleta oAtleta = new Atleta(reader.GetInt32(reader.GetOrdinal("Alt_ID")),
                            reader.GetString(reader.GetOrdinal("Alt_DNI")),
                            reader.GetString(reader.GetOrdinal("Alt_Apellido")),
                            reader.GetString(reader.GetOrdinal("Alt_Nombre")),
                            reader.GetString(reader.GetOrdinal("Alt_Nacionalidad")),
                            reader.GetString(reader.GetOrdinal("Alt_Entrenador")),
                            reader.GetString(reader.GetOrdinal("Alt_Genero"))[0],
                            reader.GetDouble(reader.GetOrdinal("Alt_Altura")),
                            reader.GetDouble(reader.GetOrdinal("Alt_Peso")),
                            reader.GetDateTime(reader.GetOrdinal("Alt_FechaNac")),
                            reader.GetString(reader.GetOrdinal("Alt_Direccion")),
                            reader.GetString(reader.GetOrdinal("Alt_Email"))
                            );
                    atletas.Add(oAtleta);
                }
            }
            cnn.Close();
            return atletas;
        }
    }
}
