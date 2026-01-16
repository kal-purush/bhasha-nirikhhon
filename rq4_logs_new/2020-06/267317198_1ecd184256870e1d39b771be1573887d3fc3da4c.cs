//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using System.Data;
//using System.Data.SqlClient;

//namespace QuanLyThuVien
//{
//    class AccessData
//    {
//        public SqlConnection GetConnection()
//        {
//            //return new SqlConnection("Data Source=LAPTOP-TM216RFV\SQLEXPRESS;Initial Catalog=QuanLyThuVien;Integrated Security=True");
//        }
//        public void ExcuteNonQuery(string sql)
//        {
//            SqlConnection conn = GetConnection();
//            SqlCommand cmd = new SqlCommand(sql, conn);
//            conn.Open();
//            cmd.ExecuteNonQuery();
//            conn.Close();
//            cmd.Dispose();
//        }
//        public SqlDataReader ExecuteReader(string sql)
//        {
//            SqlConnection conn = GetConnection();
//            conn.Open();
//            SqlCommand cmd = new SqlCommand(sql, conn);
//            SqlDataReader reader = cmd.ExecuteReader();
//            return reader;
//        }
  
//    }
//}