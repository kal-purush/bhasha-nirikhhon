using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DTO;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Data;
namespace DAL
{
    public class HDV_DAL
    {

        public DataTable GetDataHDV()
        {
            SqlConnection conn = ConnectDB.ConnectData();
            ConnectDB.Open(conn);
            try
            {
                string sql = "SELECT * FROM HuongDanVien";
                SqlCommand cmd = new SqlCommand(sql, conn);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                cmd.Dispose();
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
            catch (Exception)
            {
                return null;
            }
            finally
            {
                ConnectDB.Close(conn);
            }
        }

        public Boolean ThemHDV(HDV_DTO HDV)
        {
            string sql = "INSERT INTO HuongDanVien VALUES(N'" + HDV.IDHDV + "', N'" + HDV.TenHDV + "', N'" + HDV.GioiTinh
                + "', '" + HDV.NgaySinh.ToString("yyyy-MM-dd") + "')";
            return new ExecuteDB().ExecuteData(sql);
        }

        public Boolean SuaHDV(HDV_DTO HDV)
        {
            string sql = "UPDATE HuongDanVien SET TenHDV = N'" + HDV.TenHDV + "', GioiTinh = '" + HDV.GioiTinh + "', NgaySinh = '" + HDV.NgaySinh.ToString("yyyy-MM-dd") + "'WHERE IDHDV = N'" + HDV.IDHDV + "'";
            return new ExecuteDB().ExecuteData(sql);
        }
        public Boolean XoaHDV(string idHDV)
        {
            string sql = "DELETE FROM HuongDanVien WHERE IDHDV = N'" + idHDV + "'";
            return new ExecuteDB().ExecuteData(sql);
        }
    }
}