using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Windows.Forms;
namespace WindowsFormsApplication2
{
    class connect
    {
        private static string connectionString = WindowsFormsApplication2.Properties.Settings.Default.QLKho;
        private static SqlConnection con;

        public connect()
        {

        }
        public static bool isConnect()
        {
            connectionString = WindowsFormsApplication2.Properties.Settings.Default.QLKho;
            con = new SqlConnection(connectionString);
            bool connect = false;
            try
            {
                con.Open();
                if (con.State == ConnectionState.Open)
                {
                    connect = true;
                }
                else
                {
                    throw new Exception("ko ket noi");
                }
            }
            catch (Exception ex)
            {
            }
            return connect;
        }

        public static DataTable LoadLoaiHang()
        {
            DataTable table = null;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "select * from LoaiHang";
                com.CommandType = CommandType.Text;
                com.Connection = con;
                SqlDataAdapter adap = new SqlDataAdapter(com);
                table = new DataTable();
                adap.Fill(table);

            }
            return table;
        }

        public static bool NhapHang(Hang h)
        {
            bool nhap = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "Nhap_Hang";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ten", h.Ten);
                com.Parameters.AddWithValue("@maloai", h.Loai);
                com.Parameters.AddWithValue("@nguon", h.nguon);
                com.Parameters.AddWithValue("@soluong", h.sl);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                nhap = i > 0 ? true : false;

            }
            return nhap;
        }

        public static bool PhieuNhap(string ten,int sl, DateTime tg)
        {
            bool nhap = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "Phieu_Nhap";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ten",ten);
                com.Parameters.AddWithValue("@soluong", sl);
                com.Parameters.AddWithValue("@thoigian", tg);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                nhap = i > 0 ? true : false;

            }
            return nhap;
        }
        public static DataTable TimHang(string ma)
        {
            DataTable table = null;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "select * from HangHoa where MaHang=@ma";
                com.CommandType = CommandType.Text;
                com.Parameters.AddWithValue("@ma", ma);
                com.Connection = con;
                SqlDataAdapter adap = new SqlDataAdapter(com);
                table = new DataTable();
                adap.Fill(table);

            }
            return table;
        }
        public static bool XuatHang(string ma,int sl)
        {
            bool nhap = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "Xuat_Hang";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ma", ma);
                com.Parameters.AddWithValue("@soluong", sl);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                nhap = i > 0 ? true : false;

            }
            return nhap;
        }

        public static bool PhieuXuat(string ma, int sl, DateTime tg,string noixuat)
        {
            bool nhap = false;
            if (isConnect())
            {
                SqlCommand com = new SqlCommand();
                com.CommandText = "Phieu_Xuat";
                com.CommandType = CommandType.StoredProcedure;
                com.Parameters.AddWithValue("@ma", ma);
                com.Parameters.AddWithValue("@soluong", sl);
                com.Parameters.AddWithValue("@thoigian", tg);
                com.Parameters.AddWithValue("@noixuat", noixuat);
                com.Connection = con;
                int i = com.ExecuteNonQuery();
                nhap = i > 0 ? true : false;

            }
            return nhap;
        }
    }
}