using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace gestion.cdata
{
    class EntretienTypeDb
    {

        public void ajouterEntretienType(string libelle, int nbKm)
        {
            try
            {
                string sql = "INSERT INTO entretientype (libelle, nbKm) VALUES('" + libelle + "','" + nbKm + "')";
                MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
                cmdSql.CommandText = sql;
                cmdSql.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message.ToString());
            }
        }

        public void modifierEntretienType(int id, string libelle, int nbKm)
        {
            try
            {
                string sql = "UPDATE entretientype SET libelle='" + libelle + "', nbKm='" + nbKm + "' WHERE idEntType=" + id;
                MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
                cmdSql.CommandText = sql;
                cmdSql.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message.ToString());
            }
        }

        public void supprimerEntretienType(int id)
        {
            try
            {
                string sql = "DELETE  * FROM entretientype WHERE idEntType=" + id;
                MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
                cmdSql.CommandText = sql;
                cmdSql.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message.ToString());
            }
        }

        public List<cmetier.EntretienType> getLesTypeEntretien()
        {
            List<cmetier.EntretienType> listEntretienType = new List<cmetier.EntretienType>();
            cmetier.EntretienType entretienType;

            string sql = "SELECT * FROM entretientype";
            MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
            cmdSql.CommandText = sql;
            MySqlDataReader resultat = cmdSql.ExecuteReader();
            while (resultat.Read())
            {
                entretienType = new cmetier.EntretienType(Convert.ToInt32(resultat.GetValue(0)), resultat.GetValue(1).ToString(), Convert.ToInt32(resultat.GetValue(2)));
                listEntretienType.Add(entretienType);
            }
            resultat.Close();

            return listEntretienType;
        }

        public string getLibelleFromId(int id)
        {
            string sql = "SELECT libelle FROM entretientype WHERE idEntType=" + id;
            MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
            cmdSql.CommandText = sql;
            MySqlDataReader resultat = cmdSql.ExecuteReader();
            resultat.Read();
            string libelle = resultat.GetValue(0).ToString();
            resultat.Close();
            return libelle;
        }

        public int getIdFromLibelle(string libelle)
        {
            string sql = "SELECT idEntType FROM entretientype WHERE libelle='" + libelle + "'";
            MySqlCommand cmdSql = form_gestion.instance.CreateCommand();
            cmdSql.CommandText = sql;
            MySqlDataReader resultat = cmdSql.ExecuteReader();
            resultat.Read();
            int id = Convert.ToInt32(resultat.GetValue(0));
            resultat.Close();
            return id;
        }

    }
}