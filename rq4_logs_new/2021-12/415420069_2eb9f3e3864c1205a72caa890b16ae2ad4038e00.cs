using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Text;
using DAL.Interfaces;
using DAL.DTO;




namespace DAL.Contexts
{
    public class SQLContext : IPageContainer, IPage
    {

        private SqlConnection _connection;

        public SQLContext(SqlConnection configuration)
        {
            _connection = configuration;
        }


        public List<PageDTO> GetallText()
        {
            List<PageDTO> allText = new List<PageDTO>();



            _connection.Open();
            SqlCommand command = new SqlCommand("SELECT page_id, title, text, created_at, updated_at FROM pages", _connection);
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        allText.Add(new PageDTO
                        {
                            ID = Convert.ToInt32(reader["page_id"].ToString()),
                            Title = reader["title"].ToString(),
                            Text = reader["text"].ToString(),
                            created_at = (DateTime)reader["created_at"],
                            updated_at = (DateTime)reader["updated_at"]
                        });
                    }
                }
            }
            _connection.Close();
            return allText;
        }

        /// <summary>
        /// Useful for getting a single page.
        /// </summary>
        /// <param name="ID"></param>
        /// <returns>Returns the ID, Title and Text of a page.</returns>
        public PageDTO GetPage(int ID)
        {


            _connection.Open();
            SqlCommand command = new SqlCommand("SELECT page_id, title, text FROM pages WHERE page_id=@ID", _connection);
            {
                command.Parameters.AddWithValue("ID", ID);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        PageDTO page = new PageDTO
                        {
                            ID = Convert.ToInt32(reader["page_id"].ToString()),
                            Title = reader["title"].ToString(),
                            Text = reader["text"].ToString()
                        };
                        _connection.Close();
                        return page;
                    }
                }
            }
            _connection.Close();
            return null;
        }

        /// <summary>
        /// Creates a page in the PageDAL.
        /// </summary>
        /// <param name="page"></param>
        public void CreatePage(PageDTO page)
        {

            _connection.Open();
            SqlCommand command = new SqlCommand("INSERT INTO pages (title, text, created_at, updated_at) VALUES (@Title, @Text, @created_at, @updated_at)", _connection);
            {
                if (page.Title == null)
                {
                    command.Parameters.AddWithValue("Title", DBNull.Value);
                }
                else
                {
                    command.Parameters.AddWithValue("Title", page.Title);
                }
                if (page.Text == null)
                {
                    command.Parameters.AddWithValue("Text", DBNull.Value);
                }
                else
                {
                    command.Parameters.AddWithValue("Text", page.Text);
                }

                command.Parameters.AddWithValue("created_at", DateTime.Now);
                command.Parameters.AddWithValue("updated_at", DateTime.Now);

                command.ExecuteNonQuery();
            }
            _connection.Close();
        }


        public void DeletePage(int ID)
        {

            _connection.Open();
            SqlCommand command = new SqlCommand("DELETE FROM pages WHERE page_id=@ID", _connection);
            {
                command.Parameters.AddWithValue("ID", ID);

                command.ExecuteNonQuery();
            }
            _connection.Close();
        }

        public void EditPage(PageDTO page)
        {

            _connection.Open();
            SqlCommand command = new SqlCommand("UPDATE pages SET title= @Title, text = @Text, updated_at= @updated_at WHERE page_id=@ID", _connection);
            {
                if (page.Title == null)
                {
                    command.Parameters.AddWithValue("Title", DBNull.Value);
                }
                else
                {
                    command.Parameters.AddWithValue("Title", page.Title);
                }
                if (page.Text == null)
                {
                    command.Parameters.AddWithValue("Text", DBNull.Value);
                }
                else
                {
                    command.Parameters.AddWithValue("Text", page.Text);
                }
                command.Parameters.AddWithValue("ID", page.ID);
                command.Parameters.AddWithValue("updated_at", DateTime.Now);

                command.ExecuteNonQuery();
            }
            _connection.Close();
        }

    }
}