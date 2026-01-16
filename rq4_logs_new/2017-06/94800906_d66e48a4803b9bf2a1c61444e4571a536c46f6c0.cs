using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System;

namespace GroupProject
{
  public class Organization
  {
    private int _id;
    private string _name;
    private string _website;
    private string _email;
    private string _bio;

    public Organization(string Name, string Website, string Email, string Bio, int Id = 0)
    {
      _id = Id;
      _name = Name;
      _website = Website;
      _email = Email;
      _bio = Bio;
    }

    public int GetId()
    {
      return _id;
    }
    public string GetName()
    {
      return _name;
    }
    public string GetWebsite()
    {
      return _website;
    }
    public string GetEmail()
    {
      return _email;
    }
    public string GetBio()
    {
      return _bio;
    }

    public override bool Equals(System.Object otherOrganization)
    {
      if(!(otherOrganization is Organization))
      {
        return false;
      }
      else
      {
        Organization newOrganization = (Organization) otherOrganization;
        bool idEquality = (this.GetId() == newOrganization.GetId());
        bool nameEquality = (this.GetName() == newOrganization.GetName());
        bool websiteEquality = (this.GetWebsite() == newOrganization.GetWebsite());
        bool emailEquality = (this.GetEmail() == newOrganization.GetEmail());
        bool bioEquality = (this.GetBio() == newOrganization.GetBio());
        return (idEquality && nameEquality && websiteEquality && emailEquality && bioEquality);
      }
    }

      public override int GetHashCode()
      {
        return this.GetName().GetHashCode();
      }

    public static List<Organization> GetAll()
    {
      List<Organization> AllOrganizations = new List<Organization>{};

      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT * FROM organizations;", conn);
      SqlDataReader rdr = cmd.ExecuteReader();

      while(rdr.Read())
      {
        int id = rdr.GetInt32(0);
        string name = rdr.GetString(1);
        string website = rdr.GetString(2);
        string email = rdr.GetString(3);
        string bio = rdr.GetString(4);
        Organization newOrganization = new Organization(name, website, email, bio, id);
        AllOrganizations.Add(newOrganization);
      }
      if (rdr != null)
      {
        rdr.Close();
      }
      if (conn != null)
      {
        conn.Close();
      }

      return AllOrganizations;
    }

    public void Save()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("INSERT INTO organizations (name, website, email, bio) OUTPUT INSERTED.id VALUES (@Name, @Website, @Email, @Bio);", conn);
      SqlParameter nameParam = new SqlParameter("@Name", this.GetName());
      SqlParameter websiteParam = new SqlParameter("@Website", this.GetWebsite());
      SqlParameter emailParam = new SqlParameter("@Email", this.GetEmail());
      SqlParameter bioParam = new SqlParameter("@Bio", this.GetBio());

      cmd.Parameters.Add(nameParam);
      cmd.Parameters.Add(websiteParam);
      cmd.Parameters.Add(emailParam);
      cmd.Parameters.Add(bioParam);

      SqlDataReader rdr = cmd.ExecuteReader();

      while(rdr.Read())
      {
        this._id = rdr.GetInt32(0);  
      }
      if (rdr != null)
      {
        rdr.Close();
      }
      if (conn != null)
      {
        conn.Close();
      }
    }

    public static void DeleteAll()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();
      SqlCommand cmd = new SqlCommand("DELETE FROM organizations;", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
    }

  }
}