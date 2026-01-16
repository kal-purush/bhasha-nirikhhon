using Microsoft.Data.SqlClient;
using Roommates.Models;
using System.Collections.Generic;

namespace Roommates.Repositories
{
    /// <summary>
    /// This class is responsible for interacting with Chore data.
    /// It inherits from the BaseRepository class so it can use the BaseRepository's Connection property
    /// </summary>
    class ChoreRepository : BaseRepository
    {
        /// <summary>
        /// When new ChoreRepository is instantiated, pass the connection string along to the BaseRepository
        /// </summary>
        /// <param name="connectionString"></param>
        public ChoreRepository(string connectionString) : base(connectionString) { }

        public List<Chore> GetAll()
        {
            using (SqlConnection conn = Connection)
            {
                // Open() the connection
                conn.Open();

                // use the commands
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    // Set up SQL command
                    cmd.CommandText = @"  SELECT  Id, Name 
                                            FROM    Chore";

                    // Execute the SQL  in the database and get a "reader" that will give us access to the data.
                    SqlDataReader reader = cmd.ExecuteReader();

                    // Create a list to store the data for the chores
                    List<Chore> chores = new List<Chore>();

                    // Read() will return true as long as there is more data to read.
                    while (reader.Read())
                    {
                        // "ordinal" is the numeric position of the column in the query results.
                        int idColumnPosition = reader.GetOrdinal("Id");

                        // getXX method to get the value at that ordinal
                        int idValue = reader.GetInt32(idColumnPosition);

                        // use the same steps to get the name at that position
                        int nameColumnPosition = reader.GetOrdinal("Name");
                        string nameValue = reader.GetString(nameColumnPosition);

                        // create a chore object with the data from the database.
                        Chore chore = new Chore
                        {
                            Id = idValue,
                            Name = nameValue,
                        };

                        // add the chore to the list
                        chores.Add(chore);
                    }

                    // Close() the reader. A using block doesn't work here.
                    reader.Close();

                    // returns the list of chores to where this method was called.
                    return chores;
                }
            }
        }

        public Chore GetById(int id)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"  SELECT    Name 
                                            FROM    Chore 
                                           WHERE    Id = @id";
                    cmd.Parameters.AddWithValue("@id", id);
                    SqlDataReader reader = cmd.ExecuteReader();

                    Chore chore = null;

                    if (reader.Read())
                    {
                        chore = new Chore
                        {
                            Id = id,
                            Name = reader.GetString(reader.GetOrdinal("Name")),
                        };
                    }

                    reader.Close();

                    return chore;
                }
            }
        }

        public void Insert(Chore chore)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                { 
                    cmd.CommandText = @"INSERT  INTO Chore (Name)
                                        OUTPUT  INSERTED.Id
                                        VALUES  (@name)";
                    cmd.Parameters.AddWithValue("@name", chore.Name);
                    int id = (int)cmd.ExecuteScalar();

                    chore.Id = id;
                }
            }
        }
    }
}