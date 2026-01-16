using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace CafeManagementSystem
{
    internal class AdminAddProductsData
    {
        public int ID { get; set; } //0 
        public string ProductID { get; set; } //1
        public string ProductName { get; set; } //2
        public string Type { get; set; } //3
        public string Stock { get; set; } //4
        public string Price { get; set; } //5
        public string Status { get; set; } //6
        public string Image { get; set; } //7
        public string DateInsert { get; set; } //8 
        public string DateUpdate { get; set; } //9

        SqlConnection connect = new SqlConnection(@"Data Source=(LocalDB)\MSSQLlocaldb;Database=cafe;Integrated Security=True");

        public List<AdminAddProductsData> productsListData()
        {
            List<AdminAddProductsData> listData = new List<AdminAddProductsData>();

            if (connect.State == ConnectionState.Closed)
            {
                try
                {
                    connect.Open();

                    string selectData = "SELECT * FROM products WHERE date_delete IS NULL";

                    using (SqlCommand cmd = new SqlCommand(selectData, connect))
                    {
                        SqlDataReader reader = cmd.ExecuteReader();
                        while (reader.Read()) 
                        {
                            AdminAddProductsData apd = new AdminAddProductsData();

                            apd.ID = (int)reader["id"];
                            apd.ProductID = reader["prod_id"].ToString();
                            apd.ProductName = reader["prod_name"].ToString();
                            apd.Type = reader["prod_type"].ToString();
                            apd.Stock= reader["prod_stock"].ToString();
                            apd.Price= reader["prod_price"].ToString();
                            apd.Status = reader["prod_status"].ToString();
                            apd.Image = reader["prod_image"].ToString();
                            apd.DateInsert = reader["date_insert"].ToString();
                            apd.DateUpdate = reader["date_update"].ToString();

                            listData.Add(apd);
                        }
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine("Kết nối thất bại: " + ex);
                }
                finally
                {
                    connect.Close();
                }
            }
            return listData; 
        }

    }
}