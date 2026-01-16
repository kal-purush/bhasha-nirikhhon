using FilmSpot.Models;
using FilmSpot.Repositories;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;

namespace FilmSpot.Repository
{
    public class UserCatalogRepository : BaseRepository
    {
        public UserCatalogRepository(IConfiguration config) : base(config) { }

        public List<UserCatalog> GetUsersFavorites()
        {
            using (var conn = Connection)
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "Select Id, UserProfileId, MovieId, Favorite " +
                        "FROM UserCatalog";

                    var reader = cmd.ExecuteReader();

                    var favorites = new List<UserCatalog>();

                    while (reader.Read())
                    {
                        favorites.Add(new UserCatalog()
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id")),
                            UserProfileId = reader.GetInt32(reader.GetOrdinal("UserProfileId")),
                            MovieId = reader.GetInt32(reader.GetOrdinal("MovieId")),
                            Favorite = reader.GetBoolean(reader.GetOrdinal("Favorite"))

                        });


                    }
                    reader.Close();

                    return favorites;
                }
            }
        }

        public void AddFavorite(UserCatalog userCatalog)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"
                    INSERT INTO UserCatalog (Id, UserProfileId, MovieId, Favorite, Later FROM UserCatalog)
                    OUTPUT INSERTED.ID
                    VALUES (@id, @userProfileId, @movieId, @favorite, @later FROM UserCatalog);
                ";


                    cmd.Parameters.AddWithValue("@id", userCatalog.Id);
                    cmd.Parameters.AddWithValue("@userProfileId", userCatalog.UserProfileId);
                    cmd.Parameters.AddWithValue("@movieId", userCatalog.MovieId);
                    cmd.Parameters.AddWithValue("@favorite", userCatalog.Favorite);
                    cmd.Parameters.AddWithValue("@later", false);
                    int id = (int)cmd.ExecuteScalar();

                    userCatalog.Id = id;
                }
            }
        }
    }
}