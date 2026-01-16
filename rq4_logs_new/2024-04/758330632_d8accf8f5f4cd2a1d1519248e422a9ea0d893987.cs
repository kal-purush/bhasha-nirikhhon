using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyTraoDoiHang
{
    class OrderTableDAO
    {
        public static SqlConnection conn = new SqlConnection(Properties.Settings.Default.connStr);
        public static DBConnection dBConnection = new DBConnection();

        private static string tableName = "OrderTable";

        public static int CreateID()
        {
            string sqlStr = string.Format("SELECT * FROM " + tableName);
            DataTable x = dBConnection.Load(sqlStr);
            int maxValue = 0;
            foreach (DataRow row in x.Rows)
            {
                maxValue = Math.Max(maxValue, Convert.ToInt32(row["OrderId"]));
            }
            return maxValue + 1;
        }


        public OrderTableDAO() { }
        public static DataTable Load()
        {
            string sqlStr = string.Format("SELECT * FROM " + tableName);
            return dBConnection.Load(sqlStr);
        }


        public static void Update(ReceiveInfo receiveInfo)
        {
            string SQL = string.Format(" UPDATE " + tableName + " SET userId = '{1}', name = '{2}', phone = '{3}', address = '{4}'  WHERE receiveId = '{0}' ;",
            receiveInfo.receiveId, receiveInfo.userId, receiveInfo.name, receiveInfo.phone, receiveInfo.address);
            dBConnection.Execute(SQL);
        }
        public static void Add(ReceiveInfo receiveInfo)
        {
            string SQL = string.Format(" INSERT INTO " + tableName + " (receiveId, userId, name, phone, address) VALUES ('{0}','{1}','{2}','{3}','{4}');",
            receiveInfo.receiveId, receiveInfo.userId, receiveInfo.name, receiveInfo.phone, receiveInfo.address);
            dBConnection.Execute(SQL);
        }
        public static void Delete(ReceiveInfo receiveInfo)
        {
            string SQL = string.Format("DELETE FROM " + tableName + " WHERE receiveId = '{0}';", receiveInfo.receiveId);
            dBConnection.Execute(SQL);
        }
        public static ReceiveInfo RowToReceiveInfo(DataRow row)
        {
            ReceiveInfo receiveInfo = new ReceiveInfo();
            receiveInfo.receiveId = Convert.ToInt32(row["receiveId"]);
            receiveInfo.userId = Convert.ToInt32(row["userId"]);
            receiveInfo.name = row["name"].ToString();
            receiveInfo.phone = row["phone"].ToString();
            receiveInfo.address = row["address"].ToString();

            return receiveInfo;
        }
    }
}