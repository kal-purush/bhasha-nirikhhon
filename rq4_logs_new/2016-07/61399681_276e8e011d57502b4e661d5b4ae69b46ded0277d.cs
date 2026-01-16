using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace Ellie
{
    public class Persistencia
    {

        //Guarda aqui o nome para inserção no SQLite.
        private string createTableQuery = @"CREATE TABLE IF NOT EXISTS GameResults (
                          ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                          PlayerName NVARCHAR(2048)  NOT NULL,
                          Score VARCHAR(2048)  NULL,
                          GameDateTime VARCHAR(2048)  NULL
                          )";

        private SQLiteConnection conexao;

        public Persistencia()
        {
            if (!File.Exists("EllieDatabase.db"))
            {
                SQLiteConnection.CreateFile("EllieDatabase.db");        // Create the file which will be hosting our database
            }
                conexao = new System.Data.SQLite.SQLiteConnection("data source=EllieDatabase.db");
                
                    using (System.Data.SQLite.SQLiteCommand com = new System.Data.SQLite.SQLiteCommand(conexao))
                    {
                        conexao.Open();                            

                        com.CommandText = createTableQuery;     
                        com.ExecuteNonQuery();                 
                    }

                    conexao.Close();        
               
            
        }

        public Int32 SaveNewPlayer( String PlayerName)
        {
            Int32 idPlayer = 0;

            using (System.Data.SQLite.SQLiteCommand com = new System.Data.SQLite.SQLiteCommand(conexao))
            {
                conexao.Open();  

                com.CommandText = "INSERT INTO GameResults (PlayerName, Score) Values ('" + PlayerName + "','0')";
                com.ExecuteNonQuery();

                //Recupera o novo registro 
                com.CommandText = "Select * FROM GameResults WHERE PlayerName ='"+ PlayerName+"'";      // Select all rows from our database table

                using (System.Data.SQLite.SQLiteDataReader reader = com.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        idPlayer = Convert.ToInt32(reader["ID"]);

                        Console.WriteLine(reader["PlayerName"] + " : " + reader["Score"]);     // Display the value of the key and value column for every row
                    }
                }
                conexao.Close();        // Close the connection to the database
            }

            return idPlayer;
        }
      
    }
}