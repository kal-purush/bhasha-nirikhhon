using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using TabloidCLI.Models;
using TabloidCLI.Repositories;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TabloidCLI.Repositories
{
    public class JournalRepository : DatabaseConnector, IRepository<Journal>
    {
        public JournalRepository(string connectionString) : base(connectionString) { }

        public List<Journal> GetAll()
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand()) 
                {
                    cmd.CommandText = @"SELECT id,
                                               Title,
                                               TextContent,
                                               CreationDate
                                        FROM Journal";
                    List<Journal> journals = new List<Journal>();

                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) 
                    {
                        Journal journal = new Journal();
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id"));
                            Title = reader.GetString(reader.GetOrdinal("Title"));
                            TextContent = reader.GetString(reader.GetOrdinal("TextContent"));
                            CreationDate = reader.GetDateTime(reader.GetOrdinal("CreationDate"));
                        };
                        journals.Add(journal);
                    }
                    reader.Close();
                    return journals;
                }
            }
        }
        public void Insert(Journal journal)
        {
            using (SqlConnection conn = Connection) 
            { 
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand()) 
                {
                    cmd.CommandText = @"INSERT INTO Journal (Title, TextContent, CreationDate)
                                                    VALUES (@title, @TextContent, @creationDate)";
                    cmd.Parameters.AddWithValue("@title", journal.title);
                    cmd.Parameters.AddWithValue("@textContent", journal.textContent);
                    cmd.Parameters.AddWithValue("@creationDate", journal.creationDate);

                    cmd.ExecuteNonQuery();
                }
            }
        }

        public void Update(Journal journal)
        {
            using (SqlConnection conn = Connection) 
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand()) 
                {
                    cmd.CommandText = @"UPDATE Journal
                                            SET Title = @title,
                                                TextContent = @textContent,
                                                CreationDate = @creationDate
                                            WHERE id = @id";
                    cmd.Parameters.AddWithValue("@title", journal.Title);
                    cmd.Parameters.AddWithValue("@textContent", journal.TextContent);
                    cmd.Parameters.AddWithValue("@creationDate", journal.CreationDate);

                    cmd.ExecuteNonQuery();
                }
            }
        }

        public void Delete(int id) 
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"DELETE FROM Journal WHERE id = @id";
                    cmd.Parameters.AddWithValue("@id", id);

                    cmd.ExecuteNonQuery();
                }
            }    
        }
    }
}