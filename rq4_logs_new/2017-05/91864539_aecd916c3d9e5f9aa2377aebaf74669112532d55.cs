using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QL_BanHang.Object;

namespace QL_BanHang.Model
{
    class KhachHangMod
    {
       
            ConnectToSql con = new ConnectToSql();
            SqlCommand cmd = new SqlCommand();

            public DataTable GetData()
            {
                DataTable dt = new DataTable();
                cmd.CommandText = "select * from KhachHang";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = con.strConn;
                try
                {
                    con.OpenConnect();
                    SqlDataAdapter sda = new SqlDataAdapter(cmd);
                    sda.Fill(dt);
                    con.CloseConnection();
                }
                catch (Exception ex)
                {
                    string mes = ex.Message;
                    cmd.Dispose();
                    con.CloseConnection();
                }
                return dt;
            }

            public bool AddKhachHang(KhachHangObj KhObj)
            {
                cmd.CommandText = "Insert into KhachHang values('" + KhObj.MaKH1 + "',N'" + KhObj.TenKH1 + "',CONVERT(date,'" + KhObj.NS1.ToShortDateString() + "',103)  ,N'" + KhObj.GT1 + "',N'" + KhObj.DiaChi1 + "','" + KhObj.SDT1 + "')";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = con.strConn;
                try
                {
                    con.OpenConnect();
                    cmd.ExecuteNonQuery();
                    con.CloseConnection();
                    return true;
                }
                catch (Exception ex)
                {
                    string mes = ex.Message;
                    cmd.Dispose();
                    con.CloseConnection();
                }
                return false;
            }

            public bool DeleteKhachHang(string ma)
            {
                cmd.CommandText = "Delete KhachHang where MaKH='" + ma + "'";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = con.strConn;
                try
                {
                    con.OpenConnect();
                    cmd.ExecuteNonQuery();
                    con.CloseConnection();
                    return true;
                }
                catch (Exception ex)
                {
                    string mes = ex.Message;
                    cmd.Dispose();
                    con.CloseConnection();
                }
                return false;
            }

            public bool UpdateKhachHang(KhachHangObj KhObj)
            {
                cmd.CommandText = "Update KhachHang set TenKH=N'" + KhObj.TenKH1 + "',NS=CONVERT(date,'" + KhObj.NS1.ToShortDateString() + "',103)  ,GT=N'" + KhObj.GT1 + "',DiaChi=N'" + KhObj.DiaChi1 + "',SDT='" + KhObj.SDT1 + "' where MaKH='" + KhObj.MaKH1 + "'";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = con.strConn;
                try
                {
                    con.OpenConnect();
                    cmd.ExecuteNonQuery();
                    con.CloseConnection();
                    return true;
                }
                catch (Exception ex)
                {
                    string mes = ex.Message;
                    cmd.Dispose();
                    con.CloseConnection();
                }
                return false;
            }
            public DataTable SearchKhachHang(string MaKH)
            {
                DataTable dt = new DataTable();
                cmd.CommandText = "select * from KhachHang  where MaKH like '%" + MaKH + "%'";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = con.strConn;
                try
                {
                    con.OpenConnect();
                    SqlDataAdapter sda = new SqlDataAdapter(cmd);
                    sda.Fill(dt);
                    con.CloseConnection();
                    return dt;

                }
                catch (Exception ex)
                {
                    string mes = ex.Message;
                    cmd.Dispose();
                    con.CloseConnection();
                }
                return dt;
            }
        }
}