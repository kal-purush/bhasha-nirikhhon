using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Cinema
{
  public class Showing
  {
    private int _id;
    private int _movie_id;
    private int _theater_id;

    public Showing (int MovieId, int TheaterId, int id = 0)
    {
      _movie_id = MovieId;
      _theater_id = TheaterId;
      _id = CollectId(Movie.Find(MovieId), Theater.Find(TheaterId));
    }

    public int CollectId(Movie movie, Theater theater)
    {
      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT id FROM movies_theaters WHERE movie_id = @MovieId AND theater_id = @TheaterId;", conn);

      SqlParameter movieIdParameter = new SqlParameter();
      movieIdParameter.ParameterName = "@MovieId";
      movieIdParameter.Value = movie.GetId().ToString();
      cmd.Parameters.Add(movieIdParameter);

      SqlParameter theaterIdParameter = new SqlParameter();
      theaterIdParameter.ParameterName = "@TheaterId";
      theaterIdParameter.Value = theater.GetId().ToString();
      cmd.Parameters.Add(theaterIdParameter);
      SqlDataReader rdr = cmd.ExecuteReader();

      int foundMovieTheaterId = 0;

      while (rdr.Read())
      {
        foundMovieTheaterId = rdr.GetInt32(0);
      }

      return foundMovieTheaterId;
    }

    public override bool Equals(System.Object otherShowing)
    {
      if (!(otherShowing is Showing))
      {
        return false;
      }
      else
      {
        Showing newShowing = (Showing) otherShowing;
        bool idEquality = this.GetId() == newShowing.GetId();
        bool MovieIdEquality = this.GetMovieId() == newShowing.GetMovieId();
        bool TheaterIdEquality = this.GetTheaterId() == newShowing.GetTheaterId();
        return (idEquality && MovieIdEquality && TheaterIdEquality);
      }
    }

    public int GetId()
    {
      return _id;
    }

    public int GetMovieId()
    {
      return _movie_id;
    }
    public void SetMovieId(int newMovieId)
    {
      _movie_id = newMovieId;
    }

    public int GetTheaterId()
    {
      return _theater_id;
    }
    public void SetTheaterId(int newTheaterId)
    {
      _theater_id = newTheaterId;
    }

    // public void Save()
    // {
    //   SqlConnection conn = DB.Connection();
    //   conn.Open();
    //
    //   SqlCommand cmd = new SqlCommand("INSERT INTO movies_theaters (movie_id, theater_id) OUTPUT INSERTED.id VALUES (@MovieId, @TheaterId);", conn);
    //
    //   SqlParameter movieIdParameter = new SqlParameter();
    //   movieIdParameter.ParameterName = "@MovieId";
    //   movieIdParameter.Value = this.GetMovieId();
    //
    //   SqlParameter theaterIdParameter = new SqlParameter();
    //   theaterIdParameter.ParameterName = "@TheaterId";
    //   theaterIdParameter.Value = this.GetTheaterId();
    //
    //   cmd.Parameters.Add(movieIdParameter);
    //   cmd.Parameters.Add(theaterIdParameter);
    //   SqlDataReader rdr = cmd.ExecuteReader();
    //
    //   while(rdr.Read())
    //   {
    //     this._id = rdr.GetInt32(0);
    //   }
    //   if (rdr != null)
    //   {
    //     rdr.Close();
    //   }
    //   if(conn != null)
    //   {
    //     conn.Close();
    //   }
    // }

    public static Showing Find(int id)
    {
      SqlConnection conn = DB.Connection();
      conn.Open();
      SqlCommand cmd = new SqlCommand("SELECT * FROM movies_theaters WHERE id = @ShowingId;", conn);
      SqlParameter showingIdParameter = new SqlParameter();
      showingIdParameter.ParameterName = "@ShowingId";
      showingIdParameter.Value = id.ToString();
      cmd.Parameters.Add(showingIdParameter);

      SqlDataReader rdr = cmd.ExecuteReader();

      int showingId = 0;
      int movieId = 0;
      int theaterId = 0;

      while (rdr.Read())
      {
        showingId = rdr.GetInt32(0);
        movieId  = rdr.GetInt32(1);
        theaterId  = rdr.GetInt32(2);
      }
      Showing foundShowing = new Showing(movieId, theaterId);

      if (rdr != null)
      {
        rdr.Close();
      }
      if (conn != null)
      {
        conn.Close();
      }
      return foundShowing;
    }

    public static void DeleteAll()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("DELETE from movies_theaters;", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
    }

  }
}