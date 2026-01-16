using DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class CoSoVatChat_DAL
    {
        public DataTable GetDataCSVC()
        {
            string sql = "SELECT * FROM CoSoVatChat";
            SqlConnection conn = ConnectDB.ConnectData();
            ConnectDB.Open(conn);
            try
            {
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
                conn.Dispose();
            }
        }

        public Boolean ThemCSVC(CoSoVatChat_DTO csvc)
        {
            return new ExecuteDB().ExecuteData("INSERT INTO CoSoVatChat VALUES(N'"+ csvc.IDCSVC +"',N'"+ csvc.TenCSVC +"',N'" + csvc.MaLP +"','"+ csvc.SoLuong +"',N'"+ csvc.TinhTrang +"',N'"+ csvc.GhiChu +"')");
        }

        public Boolean SuaCSVC(CoSoVatChat_DTO csvc)
        {
            return new ExecuteDB().ExecuteData("UPDATE CoSoVatChat SET TenCSVC = N'" + csvc.TenCSVC + "', MaLP = N'" + csvc.MaLP
                + "', SoLuong = '" + csvc.SoLuong + "', TinhTrang = '" + csvc.TinhTrang + "', GhiChu = N'" + csvc.GhiChu + "'WHERE IDCSVC = N'" + csvc.IDCSVC + "'");
        }

        public Boolean XoaCSVC(string idCSVC)
        {
            return new ExecuteDB().ExecuteData("DELETE FROM CoSoVatChat WHERE IDCSVC = N'" + idCSVC + "'");
        }
    }
}