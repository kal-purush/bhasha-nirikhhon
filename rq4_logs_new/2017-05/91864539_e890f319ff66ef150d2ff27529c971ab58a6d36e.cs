using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using QL_BanHang.Object;
using QL_BanHang.Model;

namespace QL_BanHang.Model
{
    class ChiTietHoaDonMod
    {
            ConnectToSql con = new ConnectToSql();
            SqlCommand cmd = new SqlCommand();

            public DataTable GetData(string ma)
            {
                DataTable dt = new DataTable();
                cmd.CommandText = "select ct.MaHD,mh.TenMH,ct.DonGia,ct.SoLuong,ct.ThanhTien from ChiTietHoaDon ct,MatHang mh where ct.MaMH = mh.MaMH and MaHD='" + ma + "'";
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

            public bool AddChiTietHoaDon(ChiTietHoaDonObj cthdObj)
            {
                cmd.CommandText = "Insert into ChiTietHoaDon values('" + cthdObj.MaHD1 + "','" + cthdObj.MaMH1 + "','" + cthdObj.SoLuong1 + "','" + cthdObj.DonGia1 + "','" + cthdObj.ThanhTien1 + "')";
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

            public bool DeleteChiTietHoaDon(string ma)
            {
                cmd.CommandText = "Delete ChiTietHoaDon where MaHD='" + ma + "'";
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
        }
    }