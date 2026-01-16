using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

namespace WebApplication2
{
    public class Cart
    {
        

        public void addToCart(int customerID, int productID, int quantity)
        {
            //get CartID for customer
            int cartID = getCartID(customerID);
            int qty = 0;
            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\jack\Desktop\Systems Analysis Project\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True");
            
            //check if customer already has this product in their cart
            //if they do, add the quantity to the current quantity
            //if they don't add a new line for that product
            Boolean hasProduct = false;
            SqlDataReader reader;
            String sql = "SELECT productID, Quantity FROM [ShoppingCart] WHERE CustomerID = @CustomerID AND ProductID = @ProductID";

            SqlCommand command = new SqlCommand(sql, connection);

            command.Parameters.Add("@CustomerID", SqlDbType.Int);
            command.Parameters["@CustomerID"].Value = customerID;

            command.Parameters.Add("@ProductID", SqlDbType.Int);
            command.Parameters["@ProductID"].Value = productID;

            connection.Open();
            reader = command.ExecuteReader();

            if(reader.HasRows)
            {
                hasProduct = true;
                while (reader.Read())
                {
                    qty = reader.GetInt32(reader.GetOrdinal("Quantity"));
                }
            }

            reader.Close();
            connection.Close();


            if (!hasProduct)
            {
                sql = "INSERT INTO [ShoppingCart] VALUES(@CustomerID, @ProductID, @Quantity, @CartID)";
                

                    connection.Open();
                    command = new SqlCommand(sql, connection);

                    command.Parameters.Add("@CustomerID", SqlDbType.Int);
                    command.Parameters["@CustomerID"].Value = customerID;

                    command.Parameters.Add("@ProductID", SqlDbType.Int);
                    command.Parameters["@ProductID"].Value = productID;

                    command.Parameters.Add("@Quantity", SqlDbType.Int);
                    command.Parameters["@Quantity"].Value = quantity;

                    command.Parameters.Add("@CartID", SqlDbType.Int);
                    command.Parameters["@CartID"].Value = cartID;

                    command.ExecuteNonQuery();
                    connection.Close();
               
            }
            else
            {
                quantity = quantity + qty;
                sql = "UPDATE [ShoppingCart] SET Quantity = @Quantity WHERE CustomerID = @CustomerID AND CartID = @CartID AND"
                        + " ProductID = @ProductID";


                connection.Open();
                command = new SqlCommand(sql, connection);

                command.Parameters.Add("@CustomerID", SqlDbType.Int);
                command.Parameters["@CustomerID"].Value = customerID;

                command.Parameters.Add("@ProductID", SqlDbType.Int);
                command.Parameters["@ProductID"].Value = productID;

                command.Parameters.Add("@Quantity", SqlDbType.Int);
                command.Parameters["@Quantity"].Value = quantity;

                command.Parameters.Add("@CartID", SqlDbType.Int);
                command.Parameters["@CartID"].Value = cartID;

                command.ExecuteNonQuery();
                connection.Close();
                        
            }
        }

        public void removeFromCart(int customerID, int productID)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\jack\Desktop\Systems Analysis Project\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True");

            String sql = "DELETE FROM [ShoppingCart] WHERE CustomerID = @CustomerID AND ProductID = @ProductID";

            SqlCommand command = new SqlCommand(sql, connection);

            command.Parameters.Add("@CustomerID", SqlDbType.Int);
            command.Parameters["@CustomerID"].Value = customerID;

            command.Parameters.Add("@ProductID", SqlDbType.Int);
            command.Parameters["@ProductID"].Value = productID;

            connection.Open();
            command.ExecuteNonQuery();

            connection.Close();
        }
        public int getCartID(int customerID)
        {
            //check if customerID already has a cartID, if not get the max cartID, increment it and assign it to the given customerID
            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\jack\Desktop\Systems Analysis Project\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True");
            SqlDataReader reader;
            String sql = "SELECT CartID FROM [ShoppingCart] Where CustomerID = @CustomerID";
            int cartID = 0;
            connection.Open();
            SqlCommand command = new SqlCommand(sql, connection);

            command.Parameters.Add("@CustomerID", SqlDbType.Int);
            command.Parameters["@CustomerID"].Value = customerID;

            reader = command.ExecuteReader();

            while(reader.Read())
            {
              cartID = reader.GetInt32(reader.GetOrdinal("CartID"));
            }

            reader.Close();
            connection.Close();
            //if the cart ID exists, return it
            if(cartID != 0)
                return cartID;
            //if not then get the max, increment it, insert it (using addCartID), return it
            else
            {
                sql = "SELECT MAX(CartID) as MAX FROM [ShoppingCart]";
                connection.Open();
                command = new SqlCommand(sql, connection);
                reader = command.ExecuteReader();
                while(reader.Read())
                {
                    cartID = reader.GetInt32(reader.GetOrdinal("MAX"));
                }
                cartID++;
                reader.Close();
                connection.Close();
                addCartID(customerID, cartID);
                return cartID;
            }


        }

        public void addCartID(int customerID, int cartID)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=(LocalDB)\v11.0;AttachDbFilename=C:\Users\jack\Desktop\Systems Analysis Project\WebApplication2\WebApplication2\App_Data\Database.mdf;Integrated Security=True");
            String sql = "INSERT INTO [ShoppingCart] VALUES(@CustomerID, NULL, NULL, @CartID)";

            SqlCommand command = new SqlCommand(sql, connection);

            command.Parameters.Add("@CustomerID", SqlDbType.Int).Value = customerID;
            command.Parameters.Add("CartID", SqlDbType.Int).Value = cartID;
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();

        }
    }
}