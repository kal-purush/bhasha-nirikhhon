using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading.Tasks;
using Trackify.ELayer;

namespace Trackify.FLayer
{
    public class PlaylistAdapter
    {
        static SqlConnection con = new SqlConnection(Program.ConnectionString);
        public static Playlist GetPlaylistById(string playlistId)
        {
            Playlist newPlaylist = new Playlist();

            if (con.State == ConnectionState.Open)
                con.Close();
            con.Open();
            var cmd = new SqlCommand("Select * From Playlist Where PlaylistId = " + playlistId, con);
            SqlDataReader rd = cmd.ExecuteReader();
            if (!rd.Read())
            {
                return null;
            }
            newPlaylist.Id = Convert.ToInt32(rd["Id"].ToString());
            newPlaylist.PlaylistId = rd["PlaylistId"].ToString();
            newPlaylist.PlaylistUrl = rd["PlaylistURL"].ToString();
            newPlaylist.Upvote = Convert.ToInt32(rd["UpVotes"].ToString());
            newPlaylist.Downvote = Convert.ToInt32(rd["DownVotes"].ToString());
            newPlaylist.UserId = rd["UserId"].ToString();
            return newPlaylist;
        }

        public static void RemovePlaylist(Playlist playlist)
        {
            if (con.State == ConnectionState.Open)
                con.Close();
            con.Open();
            var cmd = new SqlCommand("Delete From Playlist Where PlaylistId = " + playlist.PlaylistId, con);
            cmd.ExecuteNonQuery();
            con.Close();
        }
        public static void AddPlaylist(Playlist playlist)
        {
            using (var connection = new SqlConnection(Program.ConnectionString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.CommandType = CommandType.Text;
                    command.CommandText =
                        " INSERT INTO Playlist (PlaylistId, PlaylistURL, DownVotes, UpVotes, UserId)" +

                        "VALUES(@PlaylistId, @PlaylistURL, @DownVotes, @UpVotes, @UserId)";
                    command.Parameters.AddWithValue("@PlaylistId", playlist.PlaylistId ?? SqlString.Null);
                    command.Parameters.AddWithValue("@PlaylistURL", playlist.PlaylistUrl ?? SqlString.Null);
                    command.Parameters.AddWithValue("@DownVotes", playlist.Downvote);
                    command.Parameters.AddWithValue("@UpVotes", playlist.Upvote);
                    command.Parameters.AddWithValue("@UserId", playlist.UserId ?? SqlString.Null);


                    try
                    {
                        connection.Open();
                        var recordsAffected = command.ExecuteNonQuery();
                        connection.Close();
                    }
                    catch (SqlException exception)
                    {
                        // error here
                    }
                }
            }
        }
        public static void UpdatePlaylist(Playlist playlist)
        {
            if (con.State == ConnectionState.Open)
                con.Close();
            con.Open();
            string query = "UPDATE Playlist SET " +
                "PlaylistId = \'" + playlist.PlaylistId + "\' " +
                "PlaylistURL = \'" + playlist.PlaylistUrl + "\' , " +
             "DownVotes = \'" + playlist.Downvote + "\' " +
                "UpVotes = \'" + playlist.Upvote + "\' , " +
                "ArtistId = \'" + playlist.UserId + "\'" +

                "WHERE playlistId = " + playlist.PlaylistId;
            var cmd = new SqlCommand(query, con);
            cmd.ExecuteNonQuery();
            con.Close();
        }
        public static List<Playlist> ListPlaylist()
        {
            if (con.State == System.Data.ConnectionState.Open)
                con.Close();
            var playlistList = new List<Playlist>();
            con.Open();
            var cmd = new SqlCommand("Select * From Playlist ", con);
            SqlDataReader rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var newPlaylist = new Playlist
                {
                    Id = Convert.ToInt32(rd["Id"].ToString()),
                    PlaylistId = rd["PlaylistId"].ToString(),
                    PlaylistUrl = rd["PlaylistURL"].ToString(),
                    Upvote = Convert.ToInt32(rd["UpVotes"].ToString()),
                    Downvote = Convert.ToInt32(rd["DownVotes"].ToString()),
                    UserId = rd["UserId"].ToString()
                };
                playlistList.Add(newPlaylist);
            }
            con.Close();
            return playlistList; //PlayList;
        }
    }
}