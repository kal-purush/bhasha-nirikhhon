using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Generateur
{
    public class Fabrication
    {

        private string server;
        private string userid;
        private string password;
        private string database;
        private MySqlConnection connexion;


        public Fabrication(string server, string userid, string password, string database)
        {
            this.server = server;
            this.userid = userid;
            this.password = password;
            this.database = database;
        }

        public void ConnexionOpen()
        {
            String myConnectionString = "server=" + this.server + ";uid=" + this.userid + ";pwd=" + this.password + ";database=" + this.database + ";";
            this.connexion = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                connexion.ConnectionString = myConnectionString;
                connexion.Open();
                Console.WriteLine("Connexion à la base de données établie !");
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("Impossible de se connecter à la base de données !");
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


        public void GenererLigneProduction(string name, int duree, int cadence, int changementproduit)
        {
            // Requête SQL


            MySqlCommand cmdINSERT = this.connexion.CreateCommand();

            cmdINSERT.CommandText = "INSERT INTO ligne_production (nom, duree, cadence, changement_produit) VALUES (@name, @duree, @cadence, @changement_produit)";
            // utilisation de l'objet contact passé en paramètre
            cmdINSERT.Parameters.AddWithValue("@name", name);
            cmdINSERT.Parameters.AddWithValue("@duree", duree);
            cmdINSERT.Parameters.AddWithValue("@cadence", cadence);
            cmdINSERT.Parameters.AddWithValue("@changement_produit", changementproduit);

            cmdINSERT.ExecuteNonQuery();

        }


        public void GenererProduits()
        {
            //INSERT INTO new_table(columns....)
            //SELECT columns....
            //FROM initial_table where column = value

            MySqlCommand cmdINSERT = this.connexion.CreateCommand();

            cmdINSERT.CommandText = "INSERT INTO produits.administratif (ref) SELECT Ref FROM produit.fabrication";
            // utilisation de l'objet contact passé en paramètre
            //cmdINSERT.Parameters.AddWithValue("@name", name);
            //cmdINSERT.Parameters.AddWithValue("@duree", duree);
            //cmdINSERT.Parameters.AddWithValue("@cadence", cadence);
            //cmdINSERT.Parameters.AddWithValue("@changement_produit", changementproduit);

            cmdINSERT.ExecuteNonQuery();

        }
    }
}