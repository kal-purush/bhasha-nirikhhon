using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data;

namespace ContagemInsulina
{
    internal class Conexao
    {
        //public SQLiteConnection conn = new SQLiteConnection("Data Source=D:\\DevAndroidStudio\\ContagemInsulina\\CalcInsulina.db");

        /*public void conectar()
        {
            conn.Open();
        }
        public void desconectar()
        {
            conn.Close();
        }*/

        private static SQLiteConnection sqliteConnection;

        public Conexao()

        { }
        private static SQLiteConnection DbConnection()
        {
            sqliteConnection = new SQLiteConnection("Data Source=D:\\DevAndroidStudio\\ContagemInsulina\\CalcInsulina.db");
            sqliteConnection.Open();
            return sqliteConnection;
        }
        public static void CriarBancoSQLite()
        {
            try
            {
                SQLiteConnection.CreateFile(@"D:\\DevAndroidStudio\\ContagemInsulina\\CalcInsulina.db");
            }
            catch
            {
                throw;
            }
        }

        public static DataTable GetClientes()
        {
            SQLiteDataAdapter da = null;
            DataTable dt = new DataTable();
            try
            {
                using (var cmd = DbConnection().CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM glicemias";
                    da = new SQLiteDataAdapter(cmd.CommandText, DbConnection());
                    da.Fill(dt);
                    return dt;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static DataTable GetGlicemia(int id)
        {
            SQLiteDataAdapter da = null;
            DataTable dt = new DataTable();
            try
            {
                using (var cmd = DbConnection().CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM glicemias Where id=" + id;
                    da = new SQLiteDataAdapter(cmd.CommandText, DbConnection());
                    da.Fill(dt);
                    return dt;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static void Add(Glicemia glicemia)
        {
            try
            {
                using (var cmd = DbConnection().CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO glicemia(id, valor, data ) values (@id, @valor, @data)";
                    cmd.Parameters.AddWithValue("@id", glicemia.id);
                    cmd.Parameters.AddWithValue("@valor", glicemia.valor);
                    cmd.Parameters.AddWithValue("@data", glicemia.data);
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static void Update(Glicemia glicemia)
        {
            try
            {
                using (var cmd = new SQLiteCommand(DbConnection()))
                {
                    if (glicemia.id != null)
                    {
                        cmd.CommandText = "UPDATE glicemia SET valor=@valor, data=@data WHERE id=@id";
                        cmd.Parameters.AddWithValue("@id", glicemia.id);
                        cmd.Parameters.AddWithValue("@valor", glicemia.valor);
                        cmd.Parameters.AddWithValue("@data", glicemia.data);
                        cmd.ExecuteNonQuery();
                    }
                };
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static void Delete(int Id)
        {
            try
            {
                using (var cmd = new SQLiteCommand(DbConnection()))
                {
                    cmd.CommandText = "DELETE FROM glicemia Where id=@id";
                    cmd.Parameters.AddWithValue("@id", Id);
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }



    }
}