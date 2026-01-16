using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entity;
using System.Data;

namespace DAL
{
    public class DAL_SuDungDichVu
    {
        private KetNoiData connect = new KetNoiData();

        public DataTable getDanhSach()
        {
            return connect.GetDataTable(@"select SoPhong, TenDV,ThoiGian,Gia,tblHoaDon.MaHD from tblPhong,tblPhieuThue,tblHoaDon,tblSuDungDV,tblDichVu where tblPhong.MaPh = tblPhieuThue.MaPh and tblPhieuThue.MaPhieu = tblHoaDon.MaPhieuThue and tblHoaDon.MaHD = tblSuDungDV.MaHD and tblSuDungDV.MaDV = tblDichVu.MaDV order by SoPhong,ThoiGian");
        }
        public DataTable getDanhSach(string dk)
        {
            return connect.GetDataTable(@"select SoPhong, TenDV,ThoiGian,Gia,tblHoaDon.MaHD from tblPhong,tblPhieuThue,tblHoaDon,tblSuDungDV,tblDichVu where tblPhong.MaPh = tblPhieuThue.MaPh and tblPhieuThue.MaPhieu = tblHoaDon.MaPhieuThue and tblHoaDon.MaHD = tblSuDungDV.MaHD and tblSuDungDV.MaDV = tblDichVu.MaDV " + dk + " order by SoPhong,ThoiGian");
        }

        public void addSuDung(EC_SuDungDichVu EC_SuDung)
        {
            connect.ThucHienLenh(@"insert into tblSuDungDV(MaHD,MaDV,ThoiGian) values('" + EC_SuDung.MaHD + "','" + EC_SuDung.MaDV + "','" + EC_SuDung.ThoiGian + "')");
        }

        public void delSuDung(EC_SuDungDichVu EC_SuDung)
        {
            connect.ThucHienLenh(@"delete from tblSuDungDV where MaHD = '" + EC_SuDung.MaHD + "' and MaDV = '" + EC_SuDung.MaDV + "' and ThoiGian = '" + EC_SuDung.ThoiGian + "'");
        }

        public DataTable getPhong()
        {
            return connect.GetDataTable(@"select * from tblPhong");
        }

        public DataTable getDV()
        {
            return connect.GetDataTable(@"select * from tblDichVu");
        }

        public string getMaHD(string MaPhong)
        {
            return connect.GetValue(@"select MaHD from tblHoaDon, tblPhieuThue, tblPhong where tblPhieuThue.MaPhieu = tblHoaDon.MaPhieuThue and tblPhieuThue.MaPh = tblPhong.MaPh and SoPhong = '" + MaPhong + "'");
        }
    }
}