using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data.MySqlClient;

namespace ConsoleApplication13
{
    class Program
    {
        static void Main(string[] args)
        {

            /*MySqlConnection conn;
            string myConnectionString;

            myConnectionString = "server=127.0.0.1;uid=root;" +
                "pwd=;database=client;";
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Bug");
                Console.ReadLine();
            }

            /*
            MySqlCommand cmd = conn.CreateCommand();

            // Requête SQL
            cmd.CommandText = "INSERT INTO nom (NOM) VALUES (@NOM)";

            // utilisation de l'objet contact passé en paramètre
            cmd.Parameters.AddWithValue("@NOM", "Gerad");

            // Exécution de la commande SQL
            cmd.ExecuteNonQuery();*/

            /*MySqlCommand cmd = conn.CreateCommand();

            Random rnd1 = new Random();
            int i = rnd1.Next(1,2);

            // Requête SQL
            cmd.CommandText = "SELECT NOM FROM nom WHERE ID = '"+ i + "'";


            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader[0]);
            }

            Console.ReadLine();


            conn.Close();*/

            genererreference();
            Console.WriteLine();
            //Console.WriteLine(GetRandomDate(System.DateTime.Now, System.DateTime.Now.AddMonths(-1)));
            Console.WriteLine(GetClient());
            Console.ReadLine();
        }



        void generercomande()
        {




        }

        static void genererreference()
        {
            int i;
            char c;
            Random rnd1 = new Random();
            for(int t=0;t<20;t++)
            {
                i = rnd1.Next(65, 90);
                c = Convert.ToChar(i);
                Console.Write(c);
            }
            

        }


        static readonly Random rnd = new Random();
        public static DateTime GetRandomDate(DateTime from, DateTime to)
        {
            var range = to - from;

            var randTimeSpan = new TimeSpan((long)(rnd.NextDouble() * range.Ticks));

            return from + randTimeSpan;
        }


        public static string GetClient()
        {
            MySqlConnection conn;
            string myConnectionString;

            myConnectionString = "server=127.0.0.1;uid=root;" +
                "pwd=;database=client;";
            conn = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Bug");
                Console.ReadLine();
            }


            MySqlCommand cmd = conn.CreateCommand();

            Random rnd1 = new Random();
            int i = rnd1.Next(1, 100);

            // Requête SQL
            cmd.CommandText = "SELECT NOM , PRENOM FROM nom WHERE ID = '" + i + "'";


            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                //Console.WriteLine(reader[0]+" " + reader[1]);
                return reader[0] +" "+reader[1];
            }


            return null;
        }



        public static string GetName()
        {
            MySqlConnection conn;
            string myConnectionString;

            myConnectionString = "server=127.0.0.1;uid=root;" +
                "pwd=;database=client;";
            conn = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Bug");
                Console.ReadLine();
            }


            MySqlCommand cmd = conn.CreateCommand();

            Random rnd1 = new Random();
            int i = rnd1.Next(1, 100);

            // Requête SQL
            cmd.CommandText = "SELECT NOM FROM nom WHERE ID = '" + i + "'";


            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                //Console.WriteLine(reader[0]+" " + reader[1]);
                return reader[0]+"";
            }
            conn.Close();

            return null;
        }


        public static string GetSurname()
        {
            MySqlConnection conn;
            string myConnectionString;

            myConnectionString = "server=127.0.0.1;uid=root;" +
                "pwd=;database=client;";
            conn = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Bug");
                Console.ReadLine();
            }


            MySqlCommand cmd = conn.CreateCommand();

            Random rnd1 = new Random();
            int i = rnd1.Next(1, 100);

            // Requête SQL
            cmd.CommandText = "SELECT PRENOM FROM nom WHERE ID = '" + i + "'";


            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                //Console.WriteLine(reader[0]+" " + reader[1]);
                return reader[0] + "";
            }

            conn.Close();
            return null;
        }

        public static string GetCountry()
        {
            MySqlConnection conn;
            string myConnectionString;

            myConnectionString = "server=127.0.0.1;uid=root;" +
                "pwd=;database=client;";
            conn = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Bug");
                Console.ReadLine();
            }


            MySqlCommand cmd = conn.CreateCommand();

            Random rnd1 = new Random();
            int i = rnd1.Next(1, 100);

            // Requête SQL
            cmd.CommandText = "SELECT pay FROM pay WHERE ID = '" + i + "'";


            // Exécution de la commande SQL
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                //Console.WriteLine(reader[0]+" " + reader[1]);
                return reader[0] + "";
                conn.Close();
            }

            conn.Close();

            return null;
        }

    }
}