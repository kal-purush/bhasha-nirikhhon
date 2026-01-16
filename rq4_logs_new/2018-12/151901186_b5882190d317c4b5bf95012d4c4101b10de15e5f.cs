using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using ShoppingStore.DataAccess;
using ShoppingStore.DataBase;

namespace ShoppingStore.DataAccess
{
    public static class Extras
    {

        //Method to Capitalize First letter of word.
        public static string CapitalizeFirstLetter(string nameToCapitalize)
        {
            string nameMinusOne = nameToCapitalize.Substring(1);
            string firstLetter = nameToCapitalize.Substring(0, 1).ToUpper();
            return firstLetter + nameMinusOne;
        }


        //Receipt GetList and CRUD methods.
        public static List<Receipt> GetReceiptsList()
        {
            List<Receipt> receipts = new List<Receipt>();
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                connection.Open();
                using(SqlCommand cmdList = new SqlCommand("Select * FROM Receipts", connection))
                {
                    using(SqlDataReader reader = cmdList.ExecuteReader())
                    {
                        if(reader != null)
                        {
                            while (reader.Read())
                            {
                                Receipt receipt = new Receipt
                                {
                                    ReceiptID = Convert.ToInt32(reader["ReceiptId"].ToString()),
                                    UserId = Convert.ToInt32(reader["UserId"].ToString()),
                                    ReceiptDate = Convert.ToDateTime(reader["ReceiptDate"].ToString()),
                                    ReceiptTotal = Convert.ToDecimal(reader["ReceiptTotal"].ToString())
                                };
                                receipts.Add(receipt);
                            }
                        }
                    }
                }
            }
            return receipts;
        }
        /*
                         cmdCreate.ExecuteNonQuery();
                string SQLselect = "SELECT @@IDENTITY FROM Users";
                SqlCommand cmdSelect = new SqlCommand(SQLselect, connection);
                int userId = Convert.ToInt32(cmdSelect.ExecuteScalar());
                return userId;
         * */
        //Create Receipt int or void (Receipt receipt)
        public static int CreateReceipt(int userid, decimal rtotal, DateTime rdate)
        {
            int result = -1;
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlCreate = "INSERT INTO Orders(UserId, ReceiptDate, ReceiptTotal) " +
                                               "VALUES(@uid, @date, @rtotal)";
                using(SqlCommand cmdCreate = new SqlCommand(sqlCreate, connection))
                {
                    //cmdCreate.Parameters.AddWithValue("@rid", receipt.ReceiptID);
                    cmdCreate.Parameters.AddWithValue("@uid", userid);
                    cmdCreate.Parameters.AddWithValue("@date", rdate);
                    cmdCreate.Parameters.AddWithValue("rtotal", rtotal);
                    try
                    {
                        connection.Open();
                        string sqlSELECT = "SELECT @@IDENTITY";// FROM Orders";
                        cmdCreate.ExecuteNonQuery();
                        SqlCommand cmdSelect = new SqlCommand(sqlSELECT, connection);
                        result = Convert.ToInt32(cmdSelect.ExecuteScalar());

                    }
                    catch(Exception ex) { throw ex; }
                }                
            }
            return result;
        }

        //Create product in OrderList that links with Orders
        public static void CreateProductInOrder(int receiptid, int userid, string pname, decimal pprice, int pquan, decimal ptax)
        {
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlInsert = "INSERT INTO OrdersList(ReceiptId, UserId, ProductName, ProductPrice, " +
                                   "ProductQuantity, ProductTax) VALUES(@rid, @uid, @pn, @pp, @pq, @pt)";
                using(SqlCommand cmdInsert = new SqlCommand(sqlInsert, connection))
                {

                    cmdInsert.Parameters.AddWithValue("@rid", receiptid);
                    cmdInsert.Parameters.AddWithValue("@uid", userid);
                    cmdInsert.Parameters.AddWithValue("@pn", pname);
                    cmdInsert.Parameters.AddWithValue("@pp", pprice);
                    cmdInsert.Parameters.AddWithValue("@pq", pquan);
                    cmdInsert.Parameters.AddWithValue("@pt", ptax);
                    try
                    {
                        connection.Open();
                        cmdInsert.ExecuteNonQuery();
                    }
                    catch(Exception ex) { throw ex; }
                }
            }
        }

        //Read Receipt
        public static Receipt ReadReceipt(int receiptID)
        {
            Receipt receipt = new Receipt();
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlRead = "SELECT * FROM Receipts WHERE ReceiptId = @rid";
                using(SqlCommand cmdRead = new SqlCommand(sqlRead, connection))
                {
                    cmdRead.Parameters.AddWithValue("@rid", receiptID);
                    connection.Open();
                    using(SqlDataReader reader = cmdRead.ExecuteReader())
                    {
                        if(reader != null)
                        {
                            while (reader.Read())
                            {
                                receipt.ReceiptID = Convert.ToInt32(reader["ReceiptId"].ToString());
                                receipt.UserId = Convert.ToInt32(reader["UserId"].ToString());
                                receipt.ReceiptDate = Convert.ToDateTime(reader["ReceiptDate"].ToString());
                                receipt.ReceiptTotal = Convert.ToDecimal(reader["ReceiptTotal"].ToString());
                            }
                        }
                    }
                }
            }
            return receipt;
        }
        //Update receipt
        public static bool UpdateReceipt(Receipt receipt)
        {
            bool result = false;
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlUpdate = "UPDATE Receipts SET UserId = @uid, ReceiptDate =@rdate, " +
                                   "ReceiptTotal = @rtotal WHERE ReceiptId = @rid";
                using(SqlCommand cmdUpdate = new SqlCommand(sqlUpdate, connection))
                {
                    cmdUpdate.Parameters.AddWithValue("@rid", receipt.ReceiptID);
                    cmdUpdate.Parameters.AddWithValue("@uid", receipt.UserId);
                    cmdUpdate.Parameters.AddWithValue("@rdate", receipt.ReceiptDate);
                    cmdUpdate.Parameters.AddWithValue("@rtotal", receipt.ProductsTotal);
                    try
                    {
                        connection.Open();
                        cmdUpdate.ExecuteNonQuery();
                        result = true;
                    }
                    catch(Exception ex) { throw ex; }
                }
            }
            return result;
        }
             
        //Delete receipt by Id | int
        public static int DeleteReceipt(int receiptID)
        {
            int result = -1;
            using(SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlDelete = "DELETE FROM Receipts WHERE ReceiptId = @rid";
                using(SqlCommand cmdDelete = new SqlCommand(sqlDelete, connection))
                {
                    cmdDelete.Parameters.AddWithValue("@rid", receiptID);
                    try
                    {
                        connection.Open();
                        result = cmdDelete.ExecuteNonQuery();
                    }
                    catch(Exception ex) { ex.Message.ToString(); }
                }
            }
            return result;
        }

        //Retreive all products in list from order
        public static List<ReturnReceiptProductList> GetReceiptProductList(int rid)
        {
            List<ReturnReceiptProductList> productList = new List<ReturnReceiptProductList>();
            ReturnReceiptProductList returnReceipt = new ReturnReceiptProductList();
            using (SqlConnection connection = ConnectionString.GetSqlConnection())
            {
                string sqlList = "SELECT * FROM OrdersList WHERE ReceiptId = @rid";
                using (SqlCommand cmdList = new SqlCommand(sqlList, connection))
                {
                    cmdList.Parameters.AddWithValue("@rid", rid);
                    connection.Open();
                    using (SqlDataReader reader = cmdList.ExecuteReader())
                    {
                        if (reader != null)
                        {
                            while (reader.Read())
                            {
                                Product product = new Product();
                                Receipt receipt = new Receipt();
                                receipt.ReceiptID = Convert.ToInt32(reader["ReceiptId"].ToString());
                                receipt.UserId = Convert.ToInt32(reader["UserId"].ToString());
                                //product.ProductId = Convert.ToInt32(reader["ProductId"].ToString());
                                product.ProductName = reader["ProductName"].ToString();
                                product.ProductQuantity = Convert.ToInt32(reader["ProductQuantity"].ToString());
                                product.ProductPrice = Convert.ToDecimal(reader["ProductPrice"].ToString());
                                product.ProductTax = Convert.ToDecimal(reader["ProductTax"].ToString());
                                returnReceipt = new ReturnReceiptProductList(product, receipt);
                                productList.Add(returnReceipt);
                            }
                        }
                    }
                }
            }
            return productList;
        }
        //public static List<Product> GetReceiptProductList(int rid)
        //{
        //    List<Product> productList = new List<Product>();
        //    using(SqlConnection connection = ConnectionString.GetSqlConnection())
        //    {
        //        string sqlList = "SELECT * FROM OrdersList WHERE ReceiptId = @rid";
        //        using(SqlCommand cmdList = new SqlCommand(sqlList, connection))
        //        {
        //            cmdList.Parameters.AddWithValue("@rid", rid);
        //            connection.Open();
        //            using(SqlDataReader reader = cmdList.ExecuteReader())
        //            {
        //                if(reader != null)
        //                {
        //                    while (reader.Read())
        //                    {
        //                        Product product = new Product
        //                        {
        //                            ProductName = reader["ProductName"].ToString(),
        //                            ProductPrice = Convert.ToDecimal(reader["ProductPrice"].ToString()),
        //                            ProductQuantity = Convert.ToInt32(reader["ProductQuantity"].ToString()),
        //                            ProductTax
        //                        }
        //                    }
        //                }
        //            }
        //        }
        //    }
        //}
    }
    public class ReturnReceiptProductList
    {
        //Returning the values to link Product & Receipt.

        public Product Product { get; set; }

        public Receipt Receipt { get; set; }

        public ReturnReceiptProductList() { }

        public ReturnReceiptProductList(Product p, Receipt r)
        {
            Product = p;
            Receipt = r;
        }
    }
}