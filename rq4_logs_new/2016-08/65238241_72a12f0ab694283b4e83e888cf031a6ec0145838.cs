using System.Collections.Generic;
using System.Data.SqlClient;
using System;

namespace Cinema
{
  public class Order
  {
    private int _id;
    private int _showing_id;
    private int _user_id;
    private int _quantity;

    public Order (int ShowingId, int UserId, int Quantity, int Id = 0)
    {
      _id = Id;
      _showing_id = ShowingId;
      _user_id = UserId;
      _quantity = Quantity;
    }

    public override bool Equals(System.Object otherOrder)
    {
      if (!(otherOrder is Order))
      {
        return false;
      }
      else
      {
        Order newOrder = (Order) otherOrder;
        bool idEquality = this.GetId() == newOrder.GetId();
        bool ShowingIdEquality = this.GetShowingId() == newOrder.GetShowingId();
        bool UserIdEquality = this.GetUserId() == newOrder.GetUserId();
        bool QuantityEquality = this.GetQuantity() == newOrder.GetQuantity();
        return (idEquality && ShowingIdEquality && UserIdEquality && QuantityEquality);
      }
    }

    public int GetId()
    {
      return _id;
    }

    public int GetShowingId()
    {
      return _showing_id;
    }
    public void SetShowingId(int newShowingId)
    {
      _showing_id = newShowingId;
    }

    public int GetUserId()
    {
      return _user_id;
    }
    public void SetUserId(int newUserId)
    {
      _user_id = newUserId;
    }

    public int GetQuantity()
    {
      return _quantity;
    }
    public void SetQuantity(int newQuantity)
    {
      _quantity = newQuantity;
    }

    public static List<Order> GetAll()
    {
      List<Order> allorders = new List<Order>{};

      SqlConnection conn = DB.Connection();
      conn.Open();

      SqlCommand cmd = new SqlCommand("SELECT * FROM orders;", conn);
      SqlDataReader rdr = cmd.ExecuteReader();

      while (rdr.Read())
      {
        int orderId = rdr.GetInt32(0);
        int orderShowingId = rdr.GetInt32(1);
        int orderUserId = rdr.GetInt32(2);
        int orderQuantity = rdr.GetInt32(3);

        Order newOrder = new Order(orderShowingId, orderUserId, orderQuantity, orderId);
        allorders.Add(newOrder);
      }
      if (rdr != null)
      {
        rdr.Close();
      }
      if (conn != null)
      {
        conn.Close();
      }
      return allorders;
    }

    public static void DeleteAll()
    {
      SqlConnection conn = DB.Connection();
      conn.Open();
      SqlCommand cmd = new SqlCommand("DELETE FROM orders;", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
    }
  }
}