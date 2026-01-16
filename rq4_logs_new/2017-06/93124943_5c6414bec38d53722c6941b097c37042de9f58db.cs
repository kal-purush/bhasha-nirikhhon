using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data;

namespace WordReader
{
    class DbProvider
    {
        public DbProvider()
        {

        }



        internal bool SaveToDB(string path, Consultation[] consultations)
        {

            SQLiteConnection.CreateFile(path);
            SQLiteConnection connection = new SQLiteConnection(string.Format("Data Source={0};", path));

            string createTableQuery = "CREATE TABLE consultations (id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "name TEXT, subject TEXT, groop TEXT, date TEXT, time TEXT, place TEXT, addition TEXT);";

            SQLiteCommand command = new SQLiteCommand(createTableQuery, connection);

            try
            {
                connection.Open();
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
            }

            string insertRowToDB = "INSERT INTO 'consultations' ('name', 'subject', 'groop', 'date', 'time'," +
                                   "'place', 'addition') VALUES (?, ?, ?, ?, ?, ?, ?);";

            SQLiteCommand insert_command = new SQLiteCommand(insertRowToDB, connection);

            for (int i = 1; i < consultations.Length; i++)
            {
                insert_command.Parameters.Add("@Name", DbType.String);
                insert_command.Parameters.AddWithValue("@Name", consultations[i].Lecturer.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Subject", DbType.String);
                insert_command.Parameters.AddWithValue("@Subject", consultations[i].Subject.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Groop", DbType.String);
                insert_command.Parameters.AddWithValue("@Groop", consultations[i].Group.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Date", DbType.String);
                insert_command.Parameters.AddWithValue("@Date", consultations[i].Date.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Time", DbType.String);
                insert_command.Parameters.AddWithValue("@Time", consultations[i].Time.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Place", DbType.String);
                insert_command.Parameters.AddWithValue("@Place", consultations[i].Place.Trim('\r', '\a'));

                insert_command.Parameters.Add("@Addition", DbType.String);
                insert_command.Parameters.AddWithValue("@Addition", consultations[i].Addition.Trim('\r', '\a'));

                try
                {
                    insert_command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                }
            }
            connection.Close();
            return true;
        }

        internal DataTable FillDB(string pathToDB)
        {
            string databaseName = pathToDB;
            DataTable dt = null;
            SQLiteConnection connection = new SQLiteConnection(string.Format("Data Source={0};", databaseName));

            SQLiteCommand cmd = new SQLiteCommand("SELECT * FROM 'consultations'", connection);
            DataSet ds = new DataSet();

            try
            {
                connection.Open();
                cmd.ExecuteNonQuery();
                SQLiteDataAdapter da = new SQLiteDataAdapter(cmd);
                da.Fill(ds);
                dt = ds.Tables[0];
            }
            catch (Exception ex)
            {
            }
            finally
            {
                cmd.Dispose();
                connection.Close();
            }
            return dt;
        }
    }
}