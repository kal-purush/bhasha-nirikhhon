using System;
using System.Data.SQLite;
using System.Windows;

namespace Server
{
    class Database
    {

        private string connectionString;
        private SQLiteConnection con;


        public Database()
        {
            connectionString = @" Data Source = C:\Users\Antoine\OneDrive\5A\Sécurité\ClientServerOF\Server\database.db; Version = 3";
            con = new SQLiteConnection(connectionString);
        }

        public void connect()
        {
            try
            {
                con.Open();
                if (con.State == System.Data.ConnectionState.Open)
                {
                    MessageBox.Show("connection created");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        public void insertUser(string username, string password)
        {
            try
            {
                SQLiteCommand cmd = new SQLiteCommand();
                cmd.CommandText = @"INSERT INTO User (username, password, balance) VALUES (@username, @password, 10)";
                cmd.Connection = con;
                cmd.Parameters.Add(new SQLiteParameter(@"username", username));
                cmd.Parameters.Add(new SQLiteParameter(@"password", password));

                int i = cmd.ExecuteNonQuery();

                if (i == 1)
                {
                    MessageBox.Show("User added...");
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        public void updateBalance(string username, int differential)
        {
            try
            {
                SQLiteCommand cmd = new SQLiteCommand();
                cmd.CommandText = @"UPDATE User SET balance = balance + @differential WHERE username = @username";
                cmd.Connection = con;
                cmd.Parameters.Add(new SQLiteParameter(@"differential", differential));
                cmd.Parameters.Add(new SQLiteParameter(@"username", username));

                int i = cmd.ExecuteNonQuery();

                if (i == 1)
                {
                    MessageBox.Show("Balance of " + username + " update...");
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}