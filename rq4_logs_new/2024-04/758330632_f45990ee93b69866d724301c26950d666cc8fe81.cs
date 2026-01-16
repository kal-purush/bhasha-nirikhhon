using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyTraoDoiHang
{
    class OrderItemDAO
    {
        public static SqlConnection conn = new SqlConnection(Properties.Settings.Default.connStr);
        public static DBConnection dBConnection = new DBConnection();

        private static string tableName = "OrderItem";

        public OrderItemDAO() { }
        public static DataTable Load()
        {
            string sqlStr = string.Format("SELECT * FROM " + tableName);
            return dBConnection.Load(sqlStr);
        }


        public static void Add(OrderItem orderItem)
        {
            string SQL = string.Format(" INSERT INTO " + tableName + " (orderId, productId) VALUES ('{0}','{1}');",
            orderItem.orderId, orderItem.productId);
            dBConnection.Execute(SQL);
        }
        public static OrderItem RowToOrderItem(DataRow row)
        {
            OrderItem orderItem = new OrderItem();
            orderItem.orderId = Convert.ToInt32(row["orderId"]);
            orderItem.productId = Convert.ToInt32(row["productId"]);
            return orderItem;
        }
        public static DataTable SelectByOrderId(int orderId)
        {
            string sqlStr = string.Format("SELECT * FROM " + tableName + " where orderId='{1}';", orderId);
            return dBConnection.Load(sqlStr);

        }
    }
}