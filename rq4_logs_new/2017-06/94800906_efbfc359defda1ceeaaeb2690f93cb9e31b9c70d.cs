using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System;

namespace GroupProject
{
  public class Transaction
  {
    private int _id;
    private string _name;
    private decimal _money;

    public Transaction(string Name, decimal Money, int Id= 0)
    {
      _id =  Id;
      _name = Name;
      _money = Money;
    }

    public int GetId()
    {
      return _id;
    }
    public string GetName()
    {
      return _name;
    }
    public decimal GetTransaction()
    {
      return _money;
    }

    public override bool Equals(System.Object otherTransaction)
    {
      if(!(otherTransaction is Transaction))
      {
        return false;
      }
      else
      {
        Transaction newTransaction = (Transaction) otherTransaction;
        bool idEquality = (this.GetId() == newTransaction.GetId());
        bool nameEquality = (this.GetName() == newTransaction.GetName());
        bool moneyEquality = (this.GetTransaction() == newTransaction.GetTransaction());
        return (idEquality && nameEquality && moneyEquality);
      }
    }

    public override int GetHashCode()
    {
      return this.GetName().GetHashCode();
    }

    public static List<Transaction> GetAll()
    {
      List<Transaction> AllTransactions = new List<Transaction>{};

      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT * FROM banks;", conn);
      SqlDataReader rdr = cmd.ExecuteReader();

      while(rdr.Read())
      {
        int id = rdr.GetInt32(0);
        string name = rdr.GetString(1);
        decimal money = rdr.GetDecimal(2);
        Transaction newTransaction = new Transaction(name, money, id);
        AllTransactions.Add(newTransaction);
      }
      if (rdr != null)
      {
        rdr.Close();
      }
      if (conn != null)
      {
        conn.Close();
      }

      return AllTransactions;
    }

    public void Save()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("INSERT INTO banks (name, balance) OUTPUT INSERTED.id VALUES (@Name, @Balance);", conn);
      SqlParameter nameParam = new SqlParameter("@Name", this.GetName());
      SqlParameter balanceParam = new SqlParameter("@Balance", this.GetTransaction());

      cmd.Parameters.Add(nameParam);
      cmd.Parameters.Add(balanceParam);

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

    public static Transaction Find(int id)
    {
      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT * FROM banks WHERE id = @BalanceId", conn);
      SqlParameter transactionIdParam = new SqlParameter("@BalanceId", id.ToString());

      cmd.Parameters.Add(transactionIdParam);

      SqlDataReader rdr = cmd.ExecuteReader();

      int foundTransactionId = 0;
      string bankName = null;
      decimal transaction = 0;

      while(rdr.Read())
      {
        foundTransactionId = rdr.GetInt32(0);
        bankName = rdr.GetString(1);
        transaction = rdr.GetDecimal(2);
      }
      Transaction foundTransaction = new Transaction(bankName, transaction, foundTransactionId);
      if(rdr != null)
      {
        rdr.Close();
      }
      if(conn != null)
      {
        conn.Close();
      }

      return foundTransaction;
    }

    public static void DeleteAll()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();
      SqlCommand cmd = new SqlCommand("DELETE FROM banks;", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
    }

  }
}