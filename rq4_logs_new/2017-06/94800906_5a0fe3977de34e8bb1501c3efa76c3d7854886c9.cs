using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System;

namespace GroupProject
{
  public class User
  {
    private int _id;
    private string _userName;
    private string _password;
    private string _name;
    private string _email;
    private string _bio;

    public User(string UserName, string Password, string Name, string Email, string Bio, int Id=0)
    {
      _id = Id;
      _userName = UserName;
      _password = Password;
      _name = Name;
      _email = Email;
      _bio = Bio;
    }

    public int GetId()
    {
      return _id;
    }
    public string GetUserName()
    {
      return _userName;
    }
    public string GetPassword()
    {
      return _password;
    }
    public string GetName()
    {
      return _name;
    }
    public string GetEmail()
    {
      return _email;
    }
    public string GetBio()
    {
      return _bio;
    }


    public void SetUserName(string newUserName)
    {
      _userName = newUserName;
    }
    public void SetPassword(string newPassword)
    {
      _password = newPassword;
    }
    public void SetName(string newName)
    {
      _name = newName;
    }
    public void SetEmail(string newEmail)
    {
      _email = newEmail;
    }
    public void SetBio(string newBio)
    {
      _bio = newBio;
    }

    // public override int GetHashCode()
    // {
    //   return this.GetName().GetHashCode();
    // }

    public static List<User> GetAll()
    {
      List<User> allUsers = new List<User> {};

      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT * FROM users;", conn);
      SqlDataReader rdr = cmd.ExecuteReader();

      while(rdr.Read())
      {
        int userId = rdr.GetInt32(0);
        string userName = rdr.GetString(1);
        string password = rdr.GetString(2);
        string name = rdr.GetString(3);
        string email = rdr.GetString(4);
        string bio = rdr.GetString(5);
        User newUser = new User(userName, password, name, email, bio, userId);
        allUsers.Add(newUser);
      }
      if(rdr != null)
      {
        rdr.Close();
      }
      if(conn != null)
      {
        conn.Close();
      }
      return allUsers;
    }

    public static void DeleteAll()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();
      SqlCommand cmd = new SqlCommand("DELETE FROM users;", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
    }

    public override bool Equals(System.Object otherUser)
    {
      if (!(otherUser is User))
      {
        return false;
      }
      else
      {
        User newUser = (User) otherUser;
        bool idEquality = (this.GetId() == newUser.GetId());
        bool nameEquality = (this.GetName() == newUser.GetName());
        return (idEquality && nameEquality);
      }
    }
  }
}