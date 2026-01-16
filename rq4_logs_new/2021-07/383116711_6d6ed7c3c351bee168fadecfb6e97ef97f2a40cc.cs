using AnagramSolver.Contracts.Interfaces;
using AnagramSolver.Models.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace AnagramSolver.BusinessLogic.Classes
{
    public class WordDBRepository : IWordRepository
    {
        private const int wordsInPage = 100;
        private const int _minLength = 4;
        public HashSet<WordModel> GetAllWords()
        {
            SqlConnection con = new SqlConnection(@"Data Source=LT-LIT-SC-0597\MSSQLSERVER01;Initial Catalog=VocabularyDB;Integrated Security=True");
            string query = "select * from Words";
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con;
            cmd.CommandText = query;
            var wordsFromDB = new HashSet<WordModel>();

            con.Open();
            SqlDataReader rdr = cmd.ExecuteReader();
            if (rdr.HasRows)
            {
                while (rdr.Read())
                {
                    if (rdr["Word"].ToString().Length >= _minLength)
                    {
                        var word = new WordModel();
                        word.Word = rdr["Word"].ToString();
                        word.ID = (int)rdr["ID"];
                        wordsFromDB.Add(word);
                    }
                }
            }
            con.Close();

            return wordsFromDB;
        }

        public HashSet<WordModel> GetSpecificPage(int pageNumber)
        {
            var howManySkip = (pageNumber * wordsInPage) - wordsInPage;
            SqlConnection con = new SqlConnection(@"Data Source=LT-LIT-SC-0597\MSSQLSERVER01;Initial Catalog=VocabularyDB;Integrated Security=True");
            string query = "select * from Words";
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con;
            cmd.CommandText = query;
            var wordsFromDB = new HashSet<WordModel>();

            con.Open();
            SqlDataReader rdr = cmd.ExecuteReader();
            if (rdr.HasRows)
            {
                while (rdr.Read())
                {
                    if (rdr["Word"].ToString().Length >= _minLength)
                    {
                        var word = new WordModel();
                        word.Word = rdr["Word"].ToString();
                        word.ID = (int)rdr["ID"];
                        wordsFromDB.Add(word);
                    }
                }
            }
            con.Close();

            return wordsFromDB.Skip(howManySkip).Take(wordsInPage).ToHashSet();
        }
    }
}