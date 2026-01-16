using AnagramSolver.Contracts.Interfaces;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class CacheAnagram : ICacheAnagram
    {
        public List<string> GetCachedAnagram(string command)
        {
            var cachedAnagrams = new List<string>();
            var appSettings = ConfigurationManager.AppSettings;
            string connString = appSettings["ConnectionString"] ?? "Not Found";
            SqlConnection con = new(connString);
            string query = "select * from CachedWord";
            SqlCommand cmd = new();
            cmd.Connection = con;
            cmd.CommandText = query;
            con.Open();
            SqlDataReader rdr = cmd.ExecuteReader();
            if (rdr.HasRows)
            {
                while (rdr.Read())
                {
                    if (rdr["Word"].ToString().Contains(command))
                    {
                        cachedAnagrams.Add(rdr["Anagram"].ToString());
                        return cachedAnagrams;
                    }
                }
            }
            con.Close();
            return null;
        }

        public void PutAnagramToCache(string command, List<string> listOfAnagrams)
        {
            var anagramToDB = "";
            foreach(var anagram in listOfAnagrams)
            {
                anagramToDB += $"{anagram}  ";
            }

            var appSettings = ConfigurationManager.AppSettings;
            string connString = appSettings["ConnectionString"] ?? "Not Found";
            SqlConnection con = new(connString);
            con.Open();
            var cmd2 = new SqlCommand("INSERT INTO CachedWord (Word, Anagram) VALUES (@word, @anagram)", con);
            cmd2.Parameters.AddWithValue("@word", command);
            cmd2.Parameters.AddWithValue("@anagram", anagramToDB);
            cmd2.ExecuteNonQuery();

            con.Close();
        }
    }
}