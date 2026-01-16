using MySql.Data.MySqlClient;
using System;

namespace ConsoleApplication13
{
    public class BDD
    {

        private string server = "127.0.0.1";
        private string userid = "root";
        private string password = "";
        private string database = "client";
        private MySqlConnection connexion;


        public BDD(string server, string userid, string password, string database) {

            this.server = server;
            this.userid = userid;
            this.password = password;
            this.database = database;
        }

        public void ConnexionOpen() {
            String myConnectionString = "server=" + this.server + ";uid=" + this.userid + ";pwd=" + this.password + ";database=" + this.database + ";";
            this.connexion = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                connexion.ConnectionString = myConnectionString;
                connexion.Open();
                Console.WriteLine("Connexion à la base de données établie !");
                Console.ReadLine();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Impossible de se connecter à la base de données !");
                Console.ReadLine();
            }
        }

        public void ConnexionClose()
        {
            try
            {
                this.connexion.Close();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Impossible de se déconnecter de la base de données !");
                Console.ReadLine();
            }
        }

        public string GetName()
        {
            MySqlCommand cmd = this.connexion.CreateCommand();
            Random rnd1 = new Random();

            int i = rnd1.Next(1, 100);

            // Requête SQL
            cmd.CommandText = "SELECT NOM FROM nom WHERE ID = '" + i + "'";

            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                //Console.WriteLine(reader[0]+" " + reader[1]);
                return reader[0] + "";
            }

            return null;
        }

    }
}