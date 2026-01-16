using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyTraoDoiHang
{
    class DetailImageDAO
    {
        public static SqlConnection conn = new SqlConnection(Properties.Settings.Default.connStr);
        public static DBConnection dBConnection = new DBConnection();

        private static string tableName = "DetailImage";

        public static void Add(int productId, Image productImage)
        {
            byte[] imageData = MyImage.ImageToByteArray(productImage);

            using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.connStr))
            {
                connection.Open();
                string sqlStr = string.Format("INSERT INTO " + tableName + "(productId, image) VALUES ('{0}', @ImageData)",
                productId);

                SqlCommand cmd = new SqlCommand(sqlStr, connection);
                cmd.Parameters.Add("@ImageData", System.Data.SqlDbType.VarBinary, -1).Value = imageData;
                cmd.ExecuteNonQuery();
                connection.Close();

            }

        }
        public static void DeleteByProductId(int productId)
        {
            string SQL = string.Format("DELETE FROM " + tableName + " WHERE productId = '{0}';", productId);
            dBConnection.Execute(SQL);
        }
        public static List<Image> TakeListByProductId(int productId)
        {
            string sqlStr = string.Format("SELECT * FROM " + tableName + " where productId='{0}';", productId);
            DataTable x = dBConnection.Load(sqlStr);
            List<Image> list = new List<Image>();
            list.Add(Properties.Resources.empty_product);
            list.Add(Properties.Resources.empty_product);
            list.Add(Properties.Resources.empty_product);

            for (int i = 0; i < 3; i++)
            {
                if (x.Rows.Count >= i + 1)
                {
                    list[i] = MyImage.ByteArrayToImage((byte[])x.Rows[i]["image"]);
                }
            }
            return list;
        }
    }
}