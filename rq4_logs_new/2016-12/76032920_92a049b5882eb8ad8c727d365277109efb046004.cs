using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class KhachHang_DAL
    {
        string cnstr;
        SqlConnection cn;
        DataSet ds;
        private void ThemKH(string idKH, string hoKH, string tenKH, string gioitinhKH, string sdtKH, string emailKH, string diachiKH, string quoctichKH)
        {
            SqlDataAdapter da = new SqlDataAdapter();
            string add = "INSERT INTO KhachHang(IDKhachHang, Ho, Ten, GioiTinh, SDT, Email, DiaChi, QuocTich) VALUES (@idKhachHang, @ho, @ten, @gioitinh, @sdt, @email, @diachi, @quoctich)";
            SqlCommand cmd = new SqlCommand(add, cn);
            cmd.Parameters.AddWithValue("@idKhachHang", idKH);
            cmd.Parameters.AddWithValue("@ho", hoKH);
            cmd.Parameters.AddWithValue("@ten", tenKH);
            cmd.Parameters.AddWithValue("@gioitinh", gioitinhKH);
            cmd.Parameters.AddWithValue("@sdt", sdtKH);
            cmd.Parameters.AddWithValue("@email", emailKH);
            cmd.Parameters.AddWithValue("@diachi", diachiKH);
            cmd.Parameters.AddWithValue("@quoctich", quoctichKH);
        }
    }
}